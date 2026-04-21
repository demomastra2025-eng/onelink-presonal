import asyncio
import logging
import mimetypes
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import httpx
from telethon import TelegramClient, errors, events, functions, types
from telethon.sessions import StringSession

from app.callbacks import CallbackClient
from app.config import GatewaySettings
from app.media_store import MediaStore
from app.outbox import GatewayStore, OutboxEvent
from app.schemas import (
    ChannelSyncPayload,
    DeleteMessagesPayload,
    EditMessagePayload,
    MarkReadPayload,
    SendMessagePayload,
)

logger = logging.getLogger(__name__)
HISTORY_MESSAGE_EVENT = "message.imported"
CONTACT_IMPORT_EVENT = "contact.imported"
HISTORY_SYNC_CHECKPOINT_VERSION = 2
HISTORY_PROGRESS_EMIT_EVERY = 25
CONTACTS_PROGRESS_EMIT_EVERY = 25
HISTORY_OUTBOX_CATEGORY = "history_import"
CONTACTS_OUTBOX_CATEGORY = "contacts_import"


@dataclass
class RuntimeState:
    channel_id: int
    api_id: int
    api_hash: str
    phone_number: str
    callback_url: str
    webhook_secret: str
    string_session: str = ""
    runtime_state: Dict[str, Any] = field(default_factory=dict)
    connection_state: str = "disconnected"
    lifecycle_state: str = "pending_auth"
    last_error: Optional[str] = None
    pending_phone_code_hash: Optional[str] = None
    last_inbound_at: Optional[str] = None
    last_outbound_at: Optional[str] = None
    flood_wait_until: Optional[str] = None

    def snapshot(self) -> Dict[str, Any]:
        state = {
            **self.runtime_state,
            "last_inbound_at": self.last_inbound_at,
            "last_outbound_at": self.last_outbound_at,
            "flood_wait_until": self.flood_wait_until,
            "pending_phone_code_hash": self.pending_phone_code_hash,
        }
        return {
            "connection_state": self.connection_state,
            "lifecycle_state": self.lifecycle_state,
            "last_error": self.last_error,
            "string_session": self.string_session,
            "runtime_state": {k: v for k, v in state.items() if v is not None},
        }


class ChannelRuntime:
    def __init__(
        self,
        *,
        state: RuntimeState,
        settings: GatewaySettings,
        callbacks: CallbackClient,
        media_store: MediaStore,
        store: GatewayStore,
    ):
        self.state = state
        self._settings = settings
        self._callbacks = callbacks
        self._media_store = media_store
        self._store = store
        self._client: TelegramClient | None = None
        self._http_client = httpx.AsyncClient(timeout=120)
        self._lock = asyncio.Lock()
        self._handlers_registered = False
        self._session_file = f"/tmp/telegram-personal-{state.channel_id}"
        self._history_sync_task: asyncio.Task | None = None
        self._contacts_sync_task: asyncio.Task | None = None
        self._qr_login = None
        self._qr_wait_task: asyncio.Task | None = None
        self._process_started_at = datetime.now(timezone.utc)
        self._profile_photo_url_cache: Dict[str, Optional[str]] = {}
        self._outbox_delivery_task: asyncio.Task | None = None
        self._outbox_wakeup = asyncio.Event()
        self._history_delivery_emitted_count = int(
            state.runtime_state.get("history_sync_count", 0) or 0
        )
        self._contacts_delivery_emitted_count = int(
            state.runtime_state.get("contacts_sync_count", 0) or 0
        )

    async def close(self) -> None:
        await self._cancel_qr_wait_task()
        await self._cancel_contacts_sync_task()
        await self._cancel_history_sync_task()
        await self._cancel_outbox_delivery_task()
        if self._client and self._client.is_connected():
            await self._client.disconnect()
        await self._http_client.aclose()
        Path(self._session_file).unlink(missing_ok=True)

    async def sync(self, payload: ChannelSyncPayload) -> Dict[str, Any]:
        async with self._lock:
            self.state.api_id = payload.api_id
            self.state.api_hash = payload.api_hash
            self.state.phone_number = payload.phone_number
            self.state.callback_url = str(payload.callback_url)
            self.state.webhook_secret = payload.webhook_secret
            self.state.runtime_state = self._merge_runtime_state(payload.runtime_state)
            self.state.string_session = (
                payload.string_session or self.state.string_session
            )
            self._persist_runtime_state()
            self._ensure_outbox_delivery_task()
            await self._ensure_client()
            if self.state.string_session:
                await self._connect_if_needed()
                if await self._client.is_user_authorized():
                    self._schedule_post_auth_sync(reason="sync")
            return self.state.snapshot()

    async def request_login_code(self) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            await self._cancel_qr_wait_task()
            self._clear_qr_login_state()
            try:
                sent = await self._client.send_code_request(self.state.phone_number)
            except errors.SendCodeUnavailableError as error:
                message = _humanize_telegram_error(error)
                self._set_state("connected", "code_sent", message)
                await self._emit_runtime_update()
                raise ValueError(message) from error
            self.state.pending_phone_code_hash = sent.phone_code_hash
            self._set_state("connected", "code_sent", None)
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def request_qr_login(self) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            await self._cancel_qr_wait_task()
            if await self._client.is_user_authorized():
                self.state.pending_phone_code_hash = None
                self._clear_qr_login_state()
                self._set_state("connected", "connected", None)
                await self._emit_runtime_update()
                return self.state.snapshot()

            self._clear_qr_login_state()
            qr_login = await self._client.qr_login()
            self._qr_login = qr_login
            self._qr_wait_task = asyncio.create_task(self._wait_for_qr_login(qr_login))
            self.state.pending_phone_code_hash = None
            self.state.runtime_state["qr_login_url"] = qr_login.url
            self.state.runtime_state["qr_login_expires_at"] = _serialize_datetime(
                qr_login.expires
            )
            self.state.runtime_state["qr_login_state"] = "waiting_for_scan"
            self.state.runtime_state["qr_login_requested_at"] = _iso_now()
            self._set_state("connected", "qr_ready", None)
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def verify_code(self, code: str) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            try:
                await self._client.sign_in(
                    phone=self.state.phone_number,
                    code=code,
                    phone_code_hash=self.state.pending_phone_code_hash,
                )
                self.state.pending_phone_code_hash = None
                self._clear_qr_login_state()
                self.state.string_session = self._export_string_session()
                self._set_state("connected", "connected", None)
                self._schedule_post_auth_sync(reason="verify_code")
            except errors.SessionPasswordNeededError:
                self._clear_qr_login_state()
                self._set_state("connected", "password_required", None)
            except (
                errors.PhoneCodeEmptyError,
                errors.PhoneCodeExpiredError,
                errors.PhoneCodeHashEmptyError,
                errors.PhoneCodeInvalidError,
            ) as error:
                message = _humanize_telegram_error(error)
                self._set_state("connected", "code_sent", message)
                await self._emit_runtime_update()
                raise ValueError(message) from error
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def verify_password(self, password: str) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            try:
                await self._client.sign_in(password=password)
            except errors.PasswordHashInvalidError as error:
                message = _humanize_telegram_error(error)
                self._set_state("connected", "password_required", message)
                await self._emit_runtime_update()
                raise ValueError(message) from error
            self.state.string_session = self._export_string_session()
            self.state.pending_phone_code_hash = None
            self._clear_qr_login_state()
            self._set_state("connected", "connected", None)
            self._schedule_post_auth_sync(reason="verify_password")
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def reconnect(self) -> Dict[str, Any]:
        async with self._lock:
            await self._disconnect_client()
            await self._ensure_client()
            await self._connect_if_needed()
            authorized = await self._client.is_user_authorized()
            if not authorized:
                self._clear_qr_login_state()
            self._set_state(
                "connected" if authorized else "disconnected",
                "connected" if authorized else "pending_auth",
                None,
            )
            if authorized:
                self._schedule_post_auth_sync(reason="reconnect", force=True)
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def disconnect(self) -> Dict[str, Any]:
        async with self._lock:
            await self._cancel_qr_wait_task()
            self._clear_qr_login_state()
            self.state.pending_phone_code_hash = None
            await self._disconnect_client()
            self._set_state("disconnected", "disconnected", None)
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def fetch_profile_avatar(self, *, peer_user_id: str, avatar_fingerprint: str):
        try:
            return self._media_store.get_profile_avatar(
                channel_id=self.state.channel_id,
                peer_user_id=peer_user_id,
                avatar_fingerprint=avatar_fingerprint,
            )
        except FileNotFoundError:
            pass

        async with self._lock:
            try:
                return self._media_store.get_profile_avatar(
                    channel_id=self.state.channel_id,
                    peer_user_id=peer_user_id,
                    avatar_fingerprint=avatar_fingerprint,
                )
            except FileNotFoundError:
                pass

            await self._ensure_client()
            await self._connect_if_needed()
            entity = await self._resolve_entity(peer_user_id)
            if self._profile_photo_fingerprint(entity) != avatar_fingerprint:
                raise FileNotFoundError("Profile avatar not found or changed")

            avatar_stored, _ = await self._download_profile_photo_assets(
                entity,
                avatar_fingerprint=avatar_fingerprint,
                store_transient_media=False,
            )
            if avatar_stored is None:
                raise FileNotFoundError("Profile avatar not found or expired")
            return avatar_stored

    async def diagnostics(self) -> Dict[str, Any]:
        async with self._lock:
            me = None
            connected = bool(self._client and self._client.is_connected())
            authorized = False
            unread_reactions_count = 0
            if self._client and self._client.is_connected():
                try:
                    account = await self._client.get_me()
                    if account is not None:
                        me = {
                            "id": account.id,
                            "username": getattr(account, "username", None),
                            "phone": getattr(account, "phone", None),
                        }
                except Exception:
                    logger.exception("failed to fetch self diagnostics")
                authorized = await self._client.is_user_authorized()
                if authorized:
                    unread_reactions_count = (
                        await self._private_unread_reactions_count()
                    )
            return {
                "channel": self.state.snapshot(),
                "connected": connected,
                "authorized": authorized,
                "auth_state": (
                    "authorized" if authorized else self.state.lifecycle_state
                ),
                "connection_state": self.state.connection_state,
                "lifecycle_state": self.state.lifecycle_state,
                "last_inbound_at": self.state.last_inbound_at,
                "last_outbound_at": self.state.last_outbound_at,
                "flood_wait_until": self.state.flood_wait_until,
                "flood_wait_seconds": _seconds_until(self.state.flood_wait_until),
                "history_sync_state": self.state.runtime_state.get(
                    "history_sync_state"
                ),
                "last_history_sync_at": self.state.runtime_state.get(
                    "history_sync_completed_at"
                ),
                "history_sync_count": self.state.runtime_state.get(
                    "history_sync_count", 0
                ),
                "history_sync_enqueued_count": self.state.runtime_state.get(
                    "history_sync_enqueued_count", 0
                ),
                "history_sync_pending_count": self.state.runtime_state.get(
                    "history_sync_pending_count", 0
                ),
                "history_sync_dead_count": self.state.runtime_state.get(
                    "history_sync_dead_count", 0
                ),
                "history_dialog_count": self.state.runtime_state.get(
                    "history_dialog_count", 0
                ),
                "history_sync_mode": self.state.runtime_state.get("history_sync_mode"),
                "history_sync_cutoff_at": self.state.runtime_state.get(
                    "history_sync_cutoff_at"
                ),
                "history_sync_checkpoint_at": self._history_sync_checkpoint().get(
                    "last_checkpoint_at"
                ),
                "history_sync_resume_index": self._history_sync_checkpoint().get(
                    "next_dialog_index", 0
                ),
                "history_sync_resume_total": len(
                    self._history_sync_checkpoint().get("dialog_user_ids", [])
                ),
                "history_sync_last_completed_dialog_id": self._history_sync_checkpoint().get(
                    "last_completed_dialog_id"
                ),
                "contacts_sync_state": self.state.runtime_state.get(
                    "contacts_sync_state"
                ),
                "last_contacts_sync_at": self.state.runtime_state.get(
                    "contacts_sync_completed_at"
                ),
                "contacts_sync_count": self.state.runtime_state.get(
                    "contacts_sync_count", 0
                ),
                "contacts_sync_enqueued_count": self.state.runtime_state.get(
                    "contacts_sync_enqueued_count", 0
                ),
                "contacts_sync_pending_count": self.state.runtime_state.get(
                    "contacts_sync_pending_count", 0
                ),
                "contacts_sync_dead_count": self.state.runtime_state.get(
                    "contacts_sync_dead_count", 0
                ),
                "unread_reactions_count": unread_reactions_count,
                "me": me,
            }

    async def history_sync(
        self,
        force: bool = True,
        reset_cursor: bool = False,
        include_contacts: bool = False,
    ) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            authorized = await self._client.is_user_authorized()
            if not authorized:
                raise ValueError("Channel is not authorized")
            if force and not reset_cursor and self._retry_dead_history_delivery():
                await self._refresh_history_delivery_state(emit=True, force_emit=True)
                if include_contacts:
                    self._retry_dead_contacts_delivery()
                    await self._refresh_contacts_delivery_state(
                        emit=True, force_emit=True
                    )
                return self.state.snapshot()
            if include_contacts:
                self._schedule_contacts_then_history_sync(
                    reason="manual",
                    force=force or reset_cursor,
                    reset_cursor=reset_cursor,
                )
            else:
                self._schedule_history_sync(
                    reason="manual",
                    force=force or reset_cursor,
                    reset_cursor=reset_cursor,
                )
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def contacts_sync(self, force: bool = True) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            authorized = await self._client.is_user_authorized()
            if not authorized:
                raise ValueError("Channel is not authorized")
            if force and self._retry_dead_contacts_delivery():
                await self._refresh_contacts_delivery_state(emit=True, force_emit=True)
                return self.state.snapshot()
            self._schedule_contacts_sync(reason="manual", force=force)
            await self._emit_runtime_update()
            return self.state.snapshot()

    async def send_message(self, payload: SendMessagePayload) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            entity = await self._resolve_entity(payload.chat_id or payload.recipient_id)
            sent_message = None
            message_ids: list[str] = []
            reply_to = (
                int(payload.reply_to_message_id)
                if payload.reply_to_message_id
                else None
            )

            if payload.attachments:
                downloaded_attachments = [
                    (attachment, await self._download_attachment(attachment))
                    for attachment in payload.attachments
                ]
                source_paths = [
                    source_path for _, source_path in downloaded_attachments
                ]
                if self._can_send_as_album(payload.attachments):
                    sent_messages = await self._client.send_file(
                        entity,
                        source_paths,
                        caption=payload.text or "",
                        reply_to=reply_to,
                    )
                    if isinstance(sent_messages, list):
                        sent_message = sent_messages[0] if sent_messages else None
                        message_ids = [str(item.id) for item in sent_messages if item]
                    else:
                        sent_message = sent_messages
                        message_ids = [str(sent_messages.id)] if sent_messages else []
                else:
                    for index, (attachment, source_path) in enumerate(
                        downloaded_attachments
                    ):
                        sent_message = await self._client.send_file(
                            entity,
                            source_path,
                            caption=(
                                payload.text if index == 0 and payload.text else None
                            ),
                            reply_to=reply_to if index == 0 else None,
                            **self._outbound_media_kwargs(attachment),
                        )
                        if sent_message:
                            message_ids.append(str(sent_message.id))
            elif payload.text:
                sent_message = await self._client.send_message(
                    entity, payload.text, reply_to=reply_to
                )
                message_ids = [str(sent_message.id)] if sent_message else []
            else:
                raise ValueError("Either text or attachments must be provided")

            self.state.last_outbound_at = _iso_now()
            self._set_state("connected", "connected", None)
            await self._emit_runtime_update()
            return {
                "message_id": str(sent_message.id) if sent_message else None,
                "message_ids": message_ids,
                "channel": self.state.snapshot(),
            }

    async def mark_read(self, payload: MarkReadPayload) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            entity = await self._resolve_entity(payload.chat_id or payload.recipient_id)
            max_id = int(payload.max_id)
            await self._client.send_read_acknowledge(
                entity, max_id=max_id, clear_reactions=True
            )
            self._set_state("connected", "connected", None)
            await self._emit_runtime_update()
            return {"max_id": str(max_id), "channel": self.state.snapshot()}

    async def edit_message(self, payload: EditMessagePayload) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            entity = await self._resolve_entity(payload.chat_id or payload.recipient_id)
            message_id = int(payload.message_id)
            edited_message = await self._edit_existing_message(
                entity=entity,
                message_id=message_id,
                text=payload.text,
            )
            self.state.last_outbound_at = _iso_now()
            self._set_state("connected", "connected", None)
            await self._emit_runtime_update()
            return {
                "message_id": (
                    str(edited_message.id) if edited_message else str(message_id)
                ),
                "channel": self.state.snapshot(),
            }

    async def delete_messages(self, payload: DeleteMessagesPayload) -> Dict[str, Any]:
        async with self._lock:
            await self._ensure_client()
            await self._connect_if_needed()
            entity = await self._resolve_entity(payload.chat_id or payload.recipient_id)
            message_ids = [
                int(message_id)
                for message_id in payload.message_ids
                if str(message_id).strip()
            ]
            if not message_ids:
                raise ValueError("At least one message id must be provided")
            await self._client.delete_messages(entity, message_ids, revoke=True)
            self.state.last_outbound_at = _iso_now()
            self._set_state("connected", "connected", None)
            await self._emit_runtime_update()
            return {
                "message_ids": [str(message_id) for message_id in message_ids],
                "channel": self.state.snapshot(),
            }

    async def _ensure_client(self) -> None:
        if self._client is not None:
            return

        session = (
            StringSession(self.state.string_session)
            if self.state.string_session
            else self._session_file
        )
        self._client = TelegramClient(
            session,
            self.state.api_id,
            self.state.api_hash,
            device_model=self._settings.device_model,
            system_version=self._settings.system_version,
            app_version=self._settings.app_version,
            lang_code=self._settings.lang_code,
            system_lang_code=self._settings.system_lang_code,
            auto_reconnect=True,
        )
        if not self._handlers_registered:
            self._client.add_event_handler(self._on_new_message, events.NewMessage())
            self._client.add_event_handler(self._on_album, events.Album())
            self._client.add_event_handler(
                self._on_message_edited, events.MessageEdited()
            )
            self._client.add_event_handler(self._on_message_read, events.MessageRead())
            self._client.add_event_handler(
                self._on_message_reactions,
                events.Raw(types=types.UpdateMessageReactions),
            )
            self._client.add_event_handler(
                self._on_message_deleted, events.MessageDeleted()
            )
            self._handlers_registered = True

    async def _connect_if_needed(self) -> None:
        if self._client.is_connected():
            return
        try:
            self._set_state("connecting", self.state.lifecycle_state, None)
            await self._client.connect()
            authorized = await self._client.is_user_authorized()
            self._set_state(
                "connected" if authorized else "disconnected",
                "connected" if authorized else "pending_auth",
                None,
            )
        except errors.FloodWaitError as error:
            self.state.flood_wait_until = _iso_from_delta(error.seconds)
            self._set_state(
                "flood_wait",
                self.state.lifecycle_state,
                f"Flood wait: {error.seconds}s",
            )
            raise
        except Exception as error:
            self._set_state("failed", self.state.lifecycle_state, str(error))
            raise

    async def _disconnect_client(self) -> None:
        await self._cancel_contacts_sync_task()
        await self._cancel_history_sync_task()
        if self._client and self._client.is_connected():
            await self._client.disconnect()

    async def _cancel_qr_wait_task(self) -> None:
        if not self._qr_wait_task or self._qr_wait_task.done():
            self._qr_wait_task = None
            self._qr_login = None
            return
        self._qr_wait_task.cancel()
        try:
            await self._qr_wait_task
        except asyncio.CancelledError:
            pass
        self._qr_wait_task = None
        self._qr_login = None

    def _clear_qr_login_state(self) -> None:
        for key in (
            "qr_login_url",
            "qr_login_expires_at",
            "qr_login_state",
            "qr_login_requested_at",
            "qr_login_completed_at",
            "qr_login_expired_at",
            "qr_login_failed_at",
        ):
            self.state.runtime_state.pop(key, None)

    async def _wait_for_qr_login(self, qr_login: Any) -> None:
        try:
            await qr_login.wait()
            async with self._lock:
                if self._qr_login is not qr_login:
                    return
                self._qr_wait_task = None
                self._qr_login = None
                self.state.string_session = self._export_string_session()
                self.state.pending_phone_code_hash = None
                self._clear_qr_login_state()
                self.state.runtime_state["qr_login_state"] = "completed"
                self.state.runtime_state["qr_login_completed_at"] = _iso_now()
                self._set_state("connected", "connected", None)
                self._schedule_post_auth_sync(reason="qr_login")
                await self._emit_runtime_update()
        except errors.SessionPasswordNeededError:
            async with self._lock:
                if self._qr_login is not qr_login:
                    return
                self._qr_wait_task = None
                self._qr_login = None
                self._clear_qr_login_state()
                self.state.runtime_state["qr_login_state"] = "password_required"
                self._set_state("connected", "password_required", None)
                await self._emit_runtime_update()
        except asyncio.TimeoutError:
            async with self._lock:
                if self._qr_login is not qr_login:
                    return
                self._qr_wait_task = None
                self._qr_login = None
                self._clear_qr_login_state()
                self.state.runtime_state["qr_login_state"] = "expired"
                self.state.runtime_state["qr_login_expired_at"] = _iso_now()
                self._set_state("connected", "qr_expired", None)
                await self._emit_runtime_update()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            async with self._lock:
                if self._qr_login is not qr_login:
                    return
                self._qr_wait_task = None
                self._qr_login = None
                self._clear_qr_login_state()
                self.state.runtime_state["qr_login_state"] = "failed"
                self.state.runtime_state["qr_login_failed_at"] = _iso_now()
                self._set_state("failed", "failed", str(error))
                await self._emit_runtime_update()
            logger.exception(
                "qr login wait failed for channel=%s",
                self.state.channel_id,
            )

    def _set_state(
        self, connection_state: str, lifecycle_state: str, last_error: Optional[str]
    ) -> None:
        self.state.connection_state = connection_state
        self.state.lifecycle_state = lifecycle_state
        self.state.last_error = last_error

    def _persist_runtime_state(self) -> None:
        self._store.save_runtime_state(self.state.channel_id, self.state.runtime_state)

    def _ensure_outbox_delivery_task(self) -> None:
        if self._outbox_delivery_task and not self._outbox_delivery_task.done():
            return
        self._outbox_delivery_task = asyncio.create_task(self._run_outbox_delivery())

    async def _cancel_outbox_delivery_task(self) -> None:
        if not self._outbox_delivery_task or self._outbox_delivery_task.done():
            return
        self._outbox_delivery_task.cancel()
        try:
            await self._outbox_delivery_task
        except asyncio.CancelledError:
            pass

    def _history_scope_key(self) -> str:
        return str(self.state.runtime_state.get("history_sync_requested_at") or "")

    def _contacts_scope_key(self) -> str:
        return str(self.state.runtime_state.get("contacts_sync_requested_at") or "")

    def _retry_dead_history_delivery(self) -> bool:
        scope_key = self._history_scope_key()
        if not scope_key:
            return False
        retried = self._store.requeue_dead_events(
            channel_id=self.state.channel_id,
            category=HISTORY_OUTBOX_CATEGORY,
            scope_key=scope_key,
        )
        if retried > 0:
            self.state.runtime_state["history_sync_state"] = "delivering"
            self.state.runtime_state.pop("history_sync_completed_at", None)
            self._ensure_outbox_delivery_task()
            self._outbox_wakeup.set()
            return True
        return False

    def _retry_dead_contacts_delivery(self) -> bool:
        scope_key = self._contacts_scope_key()
        if not scope_key:
            return False
        retried = self._store.requeue_dead_events(
            channel_id=self.state.channel_id,
            category=CONTACTS_OUTBOX_CATEGORY,
            scope_key=scope_key,
        )
        if retried > 0:
            self.state.runtime_state["contacts_sync_state"] = "delivering"
            self.state.runtime_state.pop("contacts_sync_completed_at", None)
            self._ensure_outbox_delivery_task()
            self._outbox_wakeup.set()
            return True
        return False

    async def _enqueue_import_event(
        self,
        *,
        event: str,
        category: str,
        scope_key: str,
        idempotency_key: str,
        payload: Dict[str, Any],
    ) -> bool:
        if not scope_key:
            raise RuntimeError(f"Missing scope key for {category}")

        inserted = self._store.enqueue_event(
            channel_id=self.state.channel_id,
            event=event,
            category=category,
            scope_key=scope_key,
            idempotency_key=idempotency_key,
            callback_url=self.state.callback_url,
            webhook_secret=self.state.webhook_secret,
            payload=payload,
        )
        self._ensure_outbox_delivery_task()
        self._outbox_wakeup.set()
        return inserted

    async def _run_outbox_delivery(self) -> None:
        try:
            while True:
                due_events = self._store.due_events(
                    channel_id=self.state.channel_id,
                    limit=self._settings.outbox_batch_size,
                )
                if not due_events:
                    self._outbox_wakeup.clear()
                    try:
                        await asyncio.wait_for(
                            self._outbox_wakeup.wait(),
                            timeout=self._settings.outbox_poll_interval_seconds,
                        )
                    except asyncio.TimeoutError:
                        continue
                    continue

                for outbox_event in due_events:
                    await self._deliver_outbox_event(outbox_event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "outbox delivery worker failed for channel=%s", self.state.channel_id
            )

    async def _deliver_outbox_event(self, outbox_event: OutboxEvent) -> None:
        try:
            await self._callbacks.send_event(
                callback_url=outbox_event.callback_url,
                webhook_secret=outbox_event.webhook_secret,
                channel_id=outbox_event.channel_id,
                event=outbox_event.event,
                data=outbox_event.payload,
            )
            self._store.mark_acked(outbox_event.id)
            await self._after_outbox_ack(outbox_event)
        except Exception as error:
            next_attempt = outbox_event.delivery_attempts + 1
            message = str(error)
            if next_attempt >= self._settings.outbox_max_delivery_attempts:
                self._store.mark_dead(
                    outbox_event.id,
                    delivery_attempts=next_attempt,
                    error_message=message,
                )
                await self._after_outbox_dead_letter(
                    outbox_event, error_message=message
                )
                return

            self._store.mark_retry(
                outbox_event.id,
                delivery_attempts=next_attempt,
                next_attempt_at=_iso_from_delta(
                    self._outbox_backoff_seconds(next_attempt)
                ),
                error_message=message,
            )
            await self._after_outbox_retry(outbox_event)

    def _outbox_backoff_seconds(self, attempt: int) -> int:
        return min(300, 2 ** max(attempt - 1, 0))

    async def _after_outbox_ack(self, outbox_event: OutboxEvent) -> None:
        if outbox_event.category == HISTORY_OUTBOX_CATEGORY:
            await self._refresh_history_delivery_state(emit=True)
        elif outbox_event.category == CONTACTS_OUTBOX_CATEGORY:
            await self._refresh_contacts_delivery_state(emit=True)

    async def _after_outbox_retry(self, outbox_event: OutboxEvent) -> None:
        if outbox_event.category == HISTORY_OUTBOX_CATEGORY:
            await self._refresh_history_delivery_state()
        elif outbox_event.category == CONTACTS_OUTBOX_CATEGORY:
            await self._refresh_contacts_delivery_state()

    async def _after_outbox_dead_letter(
        self, outbox_event: OutboxEvent, *, error_message: str
    ) -> None:
        if outbox_event.category == HISTORY_OUTBOX_CATEGORY:
            await self._refresh_history_delivery_state(
                emit=True,
                force_emit=True,
                error_message=error_message,
            )
        elif outbox_event.category == CONTACTS_OUTBOX_CATEGORY:
            await self._refresh_contacts_delivery_state(
                emit=True,
                force_emit=True,
                error_message=error_message,
            )

    async def _refresh_history_delivery_state(
        self,
        *,
        emit: bool = False,
        force_emit: bool = False,
        error_message: Optional[str] = None,
    ) -> None:
        scope_key = self._history_scope_key()
        if not scope_key:
            return

        previous_state = str(self.state.runtime_state.get("history_sync_state") or "")
        previous_count = int(self.state.runtime_state.get("history_sync_count", 0) or 0)
        delivered = self._store.count_events(
            channel_id=self.state.channel_id,
            category=HISTORY_OUTBOX_CATEGORY,
            scope_key=scope_key,
            status="acked",
        )
        pending = self._store.count_events(
            channel_id=self.state.channel_id,
            category=HISTORY_OUTBOX_CATEGORY,
            scope_key=scope_key,
            status="pending",
        )
        dead = self._store.count_events(
            channel_id=self.state.channel_id,
            category=HISTORY_OUTBOX_CATEGORY,
            scope_key=scope_key,
            status="dead_letter",
        )

        self.state.runtime_state["history_sync_count"] = delivered
        self.state.runtime_state["history_sync_pending_count"] = pending
        self.state.runtime_state["history_sync_dead_count"] = dead

        if self.state.runtime_state.get("history_sync_reader_completed_at"):
            if pending > 0:
                self.state.runtime_state["history_sync_state"] = "delivering"
                self.state.runtime_state.pop("history_sync_completed_at", None)
            elif dead > 0:
                self.state.runtime_state["history_sync_state"] = "completed_with_errors"
                self.state.runtime_state["history_sync_completed_at"] = (
                    self.state.runtime_state.get("history_sync_completed_at")
                    or _iso_now()
                )
                self.state.runtime_state["history_sync_error"] = (
                    error_message or f"{dead} history callback deliveries failed"
                )
            else:
                self.state.runtime_state["history_sync_state"] = "completed"
                self.state.runtime_state["history_sync_completed_at"] = (
                    self.state.runtime_state.get("history_sync_completed_at")
                    or _iso_now()
                )
                self.state.runtime_state.pop("history_sync_error", None)

        self._persist_runtime_state()

        current_state = str(self.state.runtime_state.get("history_sync_state") or "")
        should_emit = force_emit or (
            emit
            and (
                current_state != previous_state
                or (
                    delivered != previous_count
                    and delivered > 0
                    and delivered % HISTORY_PROGRESS_EMIT_EVERY == 0
                )
            )
        )
        if should_emit:
            await self._emit_runtime_update()

    async def _refresh_contacts_delivery_state(
        self,
        *,
        emit: bool = False,
        force_emit: bool = False,
        error_message: Optional[str] = None,
    ) -> None:
        scope_key = self._contacts_scope_key()
        if not scope_key:
            return

        previous_state = str(self.state.runtime_state.get("contacts_sync_state") or "")
        previous_count = int(
            self.state.runtime_state.get("contacts_sync_count", 0) or 0
        )
        delivered = self._store.count_events(
            channel_id=self.state.channel_id,
            category=CONTACTS_OUTBOX_CATEGORY,
            scope_key=scope_key,
            status="acked",
        )
        pending = self._store.count_events(
            channel_id=self.state.channel_id,
            category=CONTACTS_OUTBOX_CATEGORY,
            scope_key=scope_key,
            status="pending",
        )
        dead = self._store.count_events(
            channel_id=self.state.channel_id,
            category=CONTACTS_OUTBOX_CATEGORY,
            scope_key=scope_key,
            status="dead_letter",
        )

        self.state.runtime_state["contacts_sync_count"] = delivered
        self.state.runtime_state["contacts_sync_pending_count"] = pending
        self.state.runtime_state["contacts_sync_dead_count"] = dead

        if self.state.runtime_state.get("contacts_sync_reader_completed_at"):
            if pending > 0:
                self.state.runtime_state["contacts_sync_state"] = "delivering"
                self.state.runtime_state.pop("contacts_sync_completed_at", None)
            elif dead > 0:
                self.state.runtime_state["contacts_sync_state"] = (
                    "completed_with_errors"
                )
                self.state.runtime_state["contacts_sync_completed_at"] = (
                    self.state.runtime_state.get("contacts_sync_completed_at")
                    or _iso_now()
                )
                self.state.runtime_state["contacts_sync_error"] = (
                    error_message or f"{dead} contact callback deliveries failed"
                )
            else:
                self.state.runtime_state["contacts_sync_state"] = "completed"
                self.state.runtime_state["contacts_sync_completed_at"] = (
                    self.state.runtime_state.get("contacts_sync_completed_at")
                    or _iso_now()
                )
                self.state.runtime_state.pop("contacts_sync_error", None)

        queued_history_started = self._maybe_schedule_history_after_contacts()
        self._persist_runtime_state()

        current_state = str(self.state.runtime_state.get("contacts_sync_state") or "")
        should_emit = force_emit or (
            emit
            and (
                current_state != previous_state
                or (
                    delivered != previous_count
                    and delivered > 0
                    and delivered % CONTACTS_PROGRESS_EMIT_EVERY == 0
                )
            )
        )
        if should_emit or queued_history_started:
            await self._emit_runtime_update()

    async def _emit_runtime_update(self) -> None:
        self._persist_runtime_state()
        try:
            await self._callbacks.send_event(
                callback_url=self.state.callback_url,
                webhook_secret=self.state.webhook_secret,
                channel_id=self.state.channel_id,
                event="runtime.updated",
                data=self.state.snapshot(),
            )
        except Exception:
            logger.exception(
                "failed to emit runtime update for channel=%s", self.state.channel_id
            )

    def _schedule_post_auth_sync(self, *, reason: str, force: bool = False) -> None:
        should_force = force or reason == "reconnect"
        should_run_contacts = reason in {
            "sync",
            "verify_code",
            "verify_password",
            "qr_login",
            "reconnect",
        }
        should_run_history = reason in {"reconnect"}

        if should_run_contacts and (
            should_force or self._should_auto_schedule_contacts_sync()
        ):
            self._schedule_contacts_sync(reason=reason, force=should_force)

        if should_run_history and (
            should_force or self._should_auto_schedule_history_sync()
        ):
            self._schedule_history_sync(reason=reason, force=should_force)

    def _schedule_history_sync(
        self, *, reason: str, force: bool = False, reset_cursor: bool = False
    ) -> None:
        if self._history_sync_task and not self._history_sync_task.done():
            if not force:
                return
            self._history_sync_task.cancel()

        if reset_cursor:
            self._reset_history_cursor()
        requested_at = self._next_history_sync_requested_at(reset_cursor=reset_cursor)
        self._clear_history_sync_terminal_state()
        self.state.runtime_state["history_sync_state"] = "scheduled"
        self.state.runtime_state["history_sync_reason"] = reason
        self.state.runtime_state["history_sync_mode"] = (
            "full" if reset_cursor else "incremental"
        )
        self.state.runtime_state["history_sync_requested_at"] = requested_at
        self.state.runtime_state["history_sync_reader_completed_at"] = None
        self._history_sync_task = asyncio.create_task(
            self._run_history_sync(reason=reason, reset_cursor=reset_cursor)
        )

    def _schedule_contacts_sync(self, *, reason: str, force: bool = False) -> None:
        if self._contacts_sync_task and not self._contacts_sync_task.done():
            if not force:
                return
            self._contacts_sync_task.cancel()

        self._clear_contacts_sync_terminal_state()
        self.state.runtime_state["contacts_sync_state"] = "scheduled"
        self.state.runtime_state["contacts_sync_reason"] = reason
        self.state.runtime_state["contacts_sync_requested_at"] = _iso_now()
        self.state.runtime_state["contacts_sync_reader_completed_at"] = None
        self._contacts_sync_task = asyncio.create_task(
            self._run_contacts_sync(reason=reason)
        )

    def _queue_history_after_contacts(
        self, *, reason: str, force: bool = False, reset_cursor: bool = False
    ) -> None:
        self.state.runtime_state["history_sync_after_contacts_reason"] = reason
        self.state.runtime_state["history_sync_after_contacts_force"] = bool(force)
        self.state.runtime_state["history_sync_after_contacts_reset_cursor"] = bool(
            reset_cursor
        )

    def _queued_history_after_contacts(self) -> Optional[Dict[str, Any]]:
        reason = self.state.runtime_state.get("history_sync_after_contacts_reason")
        if not reason:
            return None
        return {
            "reason": str(reason),
            "force": bool(
                self.state.runtime_state.get("history_sync_after_contacts_force")
            ),
            "reset_cursor": bool(
                self.state.runtime_state.get("history_sync_after_contacts_reset_cursor")
            ),
        }

    def _clear_queued_history_after_contacts(self) -> None:
        for key in (
            "history_sync_after_contacts_reason",
            "history_sync_after_contacts_force",
            "history_sync_after_contacts_reset_cursor",
        ):
            self.state.runtime_state.pop(key, None)

    def _schedule_contacts_then_history_sync(
        self, *, reason: str, force: bool = False, reset_cursor: bool = False
    ) -> None:
        should_run_contacts = (
            force
            or (
                self._contacts_sync_task is not None
                and not self._contacts_sync_task.done()
            )
            or self._should_auto_schedule_contacts_sync()
        )
        if should_run_contacts:
            self._queue_history_after_contacts(
                reason=reason,
                force=force,
                reset_cursor=reset_cursor,
            )
            self._schedule_contacts_sync(reason=reason, force=force)
            return

        self._schedule_history_sync(
            reason=reason, force=force, reset_cursor=reset_cursor
        )

    def _maybe_schedule_history_after_contacts(self) -> bool:
        queued_history = self._queued_history_after_contacts()
        if not queued_history:
            return False

        contacts_state = str(self.state.runtime_state.get("contacts_sync_state") or "")
        if contacts_state not in {"completed", "completed_with_errors"}:
            return False

        self._clear_queued_history_after_contacts()
        self._schedule_history_sync(
            reason=queued_history["reason"],
            force=queued_history["force"],
            reset_cursor=queued_history["reset_cursor"],
        )
        return True

    async def _cancel_history_sync_task(self) -> None:
        if not self._history_sync_task or self._history_sync_task.done():
            return
        self._history_sync_task.cancel()
        try:
            await self._history_sync_task
        except asyncio.CancelledError:
            pass

    async def _cancel_contacts_sync_task(self) -> None:
        if not self._contacts_sync_task or self._contacts_sync_task.done():
            return
        self._contacts_sync_task.cancel()
        try:
            await self._contacts_sync_task
        except asyncio.CancelledError:
            pass

    async def _run_history_sync(self, *, reason: str, reset_cursor: bool) -> None:
        started_at = _iso_now()
        try:
            self.state.runtime_state["history_sync_state"] = "running"
            self.state.runtime_state["history_sync_reason"] = reason
            self.state.runtime_state["history_sync_mode"] = (
                "full" if reset_cursor else "incremental"
            )
            self.state.runtime_state["history_sync_started_at"] = started_at
            self.state.runtime_state["history_sync_error"] = None
            await self._refresh_history_delivery_state()
            await self._emit_runtime_update()

            checkpoint = await self._prepare_history_sync_checkpoint(
                reason=reason,
                reset_cursor=reset_cursor,
            )
            cutoff = _parse_iso(checkpoint.get("cutoff_at"))
            if cutoff is None and not reset_cursor:
                cutoff = self._history_sync_cutoff(reset_cursor=reset_cursor)
            dialog_user_ids = [
                int(user_id)
                for user_id in checkpoint.get("dialog_user_ids", [])
                if str(user_id).strip()
            ]
            dialog_count = len(dialog_user_ids)
            enqueued_payloads = int(
                self.state.runtime_state.get("history_sync_enqueued_count", 0) or 0
            )

            self.state.runtime_state["history_dialog_count"] = dialog_count
            self.state.runtime_state["history_sync_cutoff_at"] = _serialize_datetime(
                cutoff
            )

            start_index = min(
                max(int(checkpoint.get("next_dialog_index", 0) or 0), 0),
                dialog_count,
            )

            for index in range(start_index, dialog_count):
                dialog_user_id = dialog_user_ids[index]
                dialog_cursor = checkpoint.setdefault("dialog_cursors", {}).setdefault(
                    str(dialog_user_id),
                    {},
                )
                last_enqueued_message_id = int(
                    dialog_cursor.get("last_enqueued_message_id") or 0
                )
                async for payload in self._iter_history_payloads_for_dialog_user_id(
                    dialog_user_id,
                    cutoff=cutoff,
                    reset_cursor=reset_cursor,
                    min_message_id=last_enqueued_message_id,
                ):
                    payload["imported_history"] = True
                    inserted = await self._enqueue_import_event(
                        event=HISTORY_MESSAGE_EVENT,
                        category=HISTORY_OUTBOX_CATEGORY,
                        scope_key=self._history_scope_key(),
                        idempotency_key=self._history_payload_idempotency_key(payload),
                        payload=payload,
                    )
                    last_enqueued_message_id = self._history_payload_cursor_id(payload)
                    dialog_cursor["last_enqueued_message_id"] = last_enqueued_message_id
                    dialog_cursor["last_checkpoint_at"] = _iso_now()
                    dialog_cursor["completed_at"] = None
                    if inserted:
                        enqueued_payloads += 1
                        self.state.runtime_state["history_sync_enqueued_count"] = (
                            enqueued_payloads
                        )
                    self._set_history_sync_checkpoint(checkpoint)
                    if enqueued_payloads % HISTORY_PROGRESS_EMIT_EVERY == 0:
                        await self._emit_runtime_update()

                checkpoint["next_dialog_index"] = index + 1
                checkpoint["last_completed_dialog_id"] = str(dialog_user_id)
                checkpoint["last_checkpoint_at"] = _iso_now()
                dialog_cursor["completed_at"] = _iso_now()
                self._set_history_sync_checkpoint(checkpoint)
                await self._emit_runtime_update()

            self.state.runtime_state["history_sync_reader_completed_at"] = _iso_now()
            self.state.runtime_state["history_dialog_count"] = dialog_count
            self.state.runtime_state["history_synced_until"] = _iso_now()
            checkpoint["next_dialog_index"] = dialog_count
            checkpoint["last_checkpoint_at"] = _iso_now()
            self._set_history_sync_checkpoint(checkpoint)
            await self._refresh_history_delivery_state(emit=True, force_emit=True)
        except asyncio.CancelledError:
            self.state.runtime_state["history_sync_state"] = "cancelled"
            self.state.runtime_state["history_sync_cancelled_at"] = _iso_now()
            await self._emit_runtime_update()
            raise
        except Exception as error:
            logger.exception(
                "history sync failed for channel=%s", self.state.channel_id
            )
            self.state.runtime_state["history_sync_state"] = "failed"
            self.state.runtime_state["history_sync_error"] = str(error)
            self.state.runtime_state["history_sync_failed_at"] = _iso_now()
            await self._emit_runtime_update()

    async def _run_contacts_sync(self, *, reason: str) -> None:
        started_at = _iso_now()
        try:
            self.state.runtime_state["contacts_sync_state"] = "running"
            self.state.runtime_state["contacts_sync_reason"] = reason
            self.state.runtime_state["contacts_sync_started_at"] = started_at
            self.state.runtime_state["contacts_sync_error"] = None
            await self._refresh_contacts_delivery_state()
            await self._emit_runtime_update()

            enqueued_contacts = int(
                self.state.runtime_state.get("contacts_sync_enqueued_count", 0) or 0
            )
            seen_user_ids: set[int] = set()

            if self._settings.contacts_include_saved:
                result = await self._client(
                    functions.contacts.GetContactsRequest(hash=0)
                )
                for user in getattr(result, "users", []) or []:
                    payload = await self._contact_payload_from_user(
                        user,
                        sync_source="saved_contact",
                    )
                    if payload is None:
                        continue
                    peer_user_id = int(payload["peer_user_id"])
                    if peer_user_id in seen_user_ids:
                        continue
                    inserted = await self._emit_contact_import(payload)
                    seen_user_ids.add(peer_user_id)
                    if inserted:
                        enqueued_contacts += 1
                        self.state.runtime_state["contacts_sync_enqueued_count"] = (
                            enqueued_contacts
                        )
                    if enqueued_contacts % CONTACTS_PROGRESS_EMIT_EVERY == 0:
                        await self._emit_runtime_update()

            if self._settings.contacts_include_dialogs:
                dialog_limit = (
                    self._settings.contacts_dialog_limit
                    if self._settings.contacts_dialog_limit > 0
                    else None
                )
                async for dialog in self._client.iter_dialogs(
                    limit=dialog_limit,
                    ignore_migrated=True,
                ):
                    if not self._should_sync_dialog(dialog):
                        continue
                    payload = await self._contact_payload_from_user(
                        getattr(dialog, "entity", None),
                        dialog=dialog,
                        sync_source="private_dialog",
                    )
                    if payload is None:
                        continue
                    peer_user_id = int(payload["peer_user_id"])
                    if peer_user_id in seen_user_ids:
                        continue
                    inserted = await self._emit_contact_import(payload)
                    seen_user_ids.add(peer_user_id)
                    if inserted:
                        enqueued_contacts += 1
                        self.state.runtime_state["contacts_sync_enqueued_count"] = (
                            enqueued_contacts
                        )
                    if enqueued_contacts % CONTACTS_PROGRESS_EMIT_EVERY == 0:
                        await self._emit_runtime_update()

            self.state.runtime_state["contacts_sync_reader_completed_at"] = _iso_now()
            self.state.runtime_state["contacts_synced_until"] = _iso_now()
            await self._refresh_contacts_delivery_state(emit=True, force_emit=True)
        except asyncio.CancelledError:
            self.state.runtime_state["contacts_sync_state"] = "cancelled"
            self.state.runtime_state["contacts_sync_cancelled_at"] = _iso_now()
            await self._emit_runtime_update()
            raise
        except Exception as error:
            logger.exception(
                "contacts sync failed for channel=%s", self.state.channel_id
            )
            self.state.runtime_state["contacts_sync_state"] = "failed"
            self.state.runtime_state["contacts_sync_error"] = str(error)
            self.state.runtime_state["contacts_sync_failed_at"] = _iso_now()
            await self._emit_runtime_update()

    async def _emit_contact_import(self, payload: Dict[str, Any]) -> bool:
        return await self._enqueue_import_event(
            event=CONTACT_IMPORT_EVENT,
            category=CONTACTS_OUTBOX_CATEGORY,
            scope_key=self._contacts_scope_key(),
            idempotency_key=f"contact:{payload['peer_user_id']}",
            payload=payload,
        )

    def _reset_history_cursor(self) -> None:
        for key in (
            "history_sync_count",
            "history_sync_enqueued_count",
            "history_sync_pending_count",
            "history_sync_dead_count",
            "history_dialog_count",
            "history_sync_completed_at",
            "history_sync_failed_at",
            "history_sync_cancelled_at",
            "history_sync_cutoff_at",
            "history_synced_until",
            "history_sync_reader_completed_at",
        ):
            self.state.runtime_state.pop(key, None)
        self.state.runtime_state.pop("history_sync_checkpoint", None)

    def _clear_history_sync_terminal_state(self) -> None:
        for key in (
            "history_sync_completed_at",
            "history_sync_failed_at",
            "history_sync_cancelled_at",
            "history_sync_error",
            "history_sync_reader_completed_at",
        ):
            self.state.runtime_state.pop(key, None)

    def _clear_contacts_sync_terminal_state(self) -> None:
        for key in (
            "contacts_sync_completed_at",
            "contacts_sync_failed_at",
            "contacts_sync_cancelled_at",
            "contacts_sync_error",
            "contacts_sync_reader_completed_at",
        ):
            self.state.runtime_state.pop(key, None)

    def _should_auto_schedule_history_sync(self) -> bool:
        return self._should_auto_schedule_task(
            state_key="history_sync_state",
            completed_at_key="history_sync_completed_at",
            started_at_keys=("history_sync_started_at", "history_sync_requested_at"),
        )

    def _should_auto_schedule_contacts_sync(self) -> bool:
        return self._should_auto_schedule_task(
            state_key="contacts_sync_state",
            completed_at_key="contacts_sync_completed_at",
            started_at_keys=("contacts_sync_started_at", "contacts_sync_requested_at"),
        )

    def _should_auto_schedule_task(
        self,
        *,
        state_key: str,
        completed_at_key: str,
        started_at_keys: tuple[str, ...],
    ) -> bool:
        state = str(self.state.runtime_state.get(state_key) or "").strip().lower()
        completed_at = _parse_iso(self.state.runtime_state.get(completed_at_key))
        if state == "completed":
            return completed_at is None or completed_at < self._process_started_at
        if state in {"scheduled", "running"}:
            latest_start = None
            for key in started_at_keys:
                candidate = _parse_iso(self.state.runtime_state.get(key))
                if candidate and (latest_start is None or candidate > latest_start):
                    latest_start = candidate
            return latest_start is None or latest_start < self._process_started_at
        return state in {"", "failed", "cancelled"}

    def _history_sync_cutoff(self, *, reset_cursor: bool) -> datetime | None:
        if reset_cursor:
            return None

        now = datetime.now(timezone.utc)
        synced_until = _parse_iso(self.state.runtime_state.get("history_synced_until"))
        if synced_until:
            return synced_until - timedelta(
                seconds=self._settings.history_overlap_seconds
            )
        return now - timedelta(hours=self._settings.history_lookback_hours)

    async def _prepare_history_sync_checkpoint(
        self, *, reason: str, reset_cursor: bool
    ) -> Dict[str, Any]:
        checkpoint = self._history_sync_checkpoint()
        requested_at = self.state.runtime_state.get("history_sync_requested_at")

        if (
            not reset_cursor
            and checkpoint.get("requested_at") == requested_at
            and checkpoint.get("cutoff_at")
            and checkpoint.get("dialog_user_ids")
        ):
            return checkpoint

        cutoff = self._history_sync_cutoff(reset_cursor=reset_cursor)
        dialog_user_ids: list[str] = []
        dialog_iter_options = {
            "limit": None if reset_cursor else self._settings.history_dialog_limit,
            "ignore_migrated": True,
        }
        if not reset_cursor:
            dialog_iter_options["archived"] = False

        async for dialog in self._client.iter_dialogs(**dialog_iter_options):
            if not self._should_sync_dialog(dialog):
                continue

            user_id = getattr(getattr(dialog, "entity", None), "id", None) or getattr(
                getattr(dialog, "entity", None), "user_id", None
            )
            if user_id is None:
                user_id = getattr(getattr(dialog, "peer_id", None), "user_id", None)
            if user_id is None:
                continue

            dialog_user_ids.append(str(user_id))

        checkpoint = {
            "version": HISTORY_SYNC_CHECKPOINT_VERSION,
            "requested_at": requested_at,
            "cutoff_at": _serialize_datetime(cutoff),
            "dialog_user_ids": dialog_user_ids,
            "dialog_cursors": {},
            "next_dialog_index": 0,
            "last_completed_dialog_id": None,
            "last_checkpoint_at": _iso_now(),
        }
        self.state.runtime_state["history_sync_count"] = 0
        self.state.runtime_state["history_sync_enqueued_count"] = 0
        self.state.runtime_state["history_dialog_count"] = len(dialog_user_ids)
        self.state.runtime_state["history_sync_reason"] = reason
        self.state.runtime_state["history_sync_cutoff_at"] = checkpoint["cutoff_at"]
        self._set_history_sync_checkpoint(checkpoint)
        return checkpoint

    def _next_history_sync_requested_at(self, *, reset_cursor: bool) -> str:
        if reset_cursor:
            return _iso_now()

        checkpoint = self._history_sync_checkpoint()
        dialog_user_ids = checkpoint.get("dialog_user_ids", [])
        next_dialog_index = int(checkpoint.get("next_dialog_index", 0) or 0)
        if (
            checkpoint.get("requested_at")
            and dialog_user_ids
            and next_dialog_index < len(dialog_user_ids)
        ):
            return str(checkpoint["requested_at"])

        return _iso_now()

    def _history_sync_checkpoint(self) -> Dict[str, Any]:
        checkpoint = self.state.runtime_state.get("history_sync_checkpoint")
        if not isinstance(checkpoint, dict):
            checkpoint = {}

        dialog_user_ids = checkpoint.get("dialog_user_ids")
        dialog_cursors = checkpoint.get("dialog_cursors")
        return {
            "version": int(
                checkpoint.get("version") or HISTORY_SYNC_CHECKPOINT_VERSION
            ),
            "requested_at": checkpoint.get("requested_at"),
            "cutoff_at": checkpoint.get("cutoff_at"),
            "dialog_user_ids": [
                str(user_id)
                for user_id in (
                    dialog_user_ids if isinstance(dialog_user_ids, list) else []
                )
                if str(user_id).strip()
            ],
            "dialog_cursors": self._normalize_dialog_cursors(dialog_cursors),
            "next_dialog_index": int(checkpoint.get("next_dialog_index") or 0),
            "last_completed_dialog_id": checkpoint.get("last_completed_dialog_id"),
            "last_checkpoint_at": checkpoint.get("last_checkpoint_at"),
        }

    def _set_history_sync_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        self.state.runtime_state["history_sync_checkpoint"] = (
            self._history_sync_checkpoint_from_payload(checkpoint)
        )
        self._persist_runtime_state()

    def _history_sync_checkpoint_from_payload(
        self, checkpoint: Dict[str, Any]
    ) -> Dict[str, Any]:
        dialog_user_ids = checkpoint.get("dialog_user_ids")
        return {
            "version": int(
                checkpoint.get("version") or HISTORY_SYNC_CHECKPOINT_VERSION
            ),
            "requested_at": checkpoint.get("requested_at"),
            "cutoff_at": checkpoint.get("cutoff_at"),
            "dialog_user_ids": [
                str(user_id)
                for user_id in (
                    dialog_user_ids if isinstance(dialog_user_ids, list) else []
                )
                if str(user_id).strip()
            ],
            "dialog_cursors": self._normalize_dialog_cursors(
                checkpoint.get("dialog_cursors")
            ),
            "next_dialog_index": max(int(checkpoint.get("next_dialog_index") or 0), 0),
            "last_completed_dialog_id": checkpoint.get("last_completed_dialog_id"),
            "last_checkpoint_at": checkpoint.get("last_checkpoint_at"),
        }

    def _normalize_dialog_cursors(self, value: Any) -> Dict[str, Dict[str, Any]]:
        if not isinstance(value, dict):
            return {}

        normalized: Dict[str, Dict[str, Any]] = {}
        for dialog_user_id, cursor in value.items():
            if not str(dialog_user_id).strip() or not isinstance(cursor, dict):
                continue
            normalized[str(dialog_user_id)] = {
                "last_enqueued_message_id": int(
                    cursor.get("last_enqueued_message_id") or 0
                ),
                "completed_at": cursor.get("completed_at"),
                "last_checkpoint_at": cursor.get("last_checkpoint_at"),
            }
        return normalized

    def _merge_runtime_state(
        self, incoming_state: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        incoming = dict(incoming_state or {})
        local = dict(self.state.runtime_state or {})
        if not local:
            return incoming
        if not incoming:
            return local

        merged = dict(local)
        for key, value in incoming.items():
            if key not in merged:
                merged[key] = value

        for prefix in ("history_sync", "contacts_sync", "qr_login"):
            if self._prefixed_runtime_state_is_newer(incoming, local, prefix):
                merged = self._overlay_prefixed_runtime_state(merged, incoming, prefix)

        return merged

    def _prefixed_runtime_state_is_newer(
        self, candidate: Dict[str, Any], baseline: Dict[str, Any], prefix: str
    ) -> bool:
        return self._prefixed_runtime_state_timestamp(
            candidate, prefix
        ) > self._prefixed_runtime_state_timestamp(baseline, prefix)

    def _prefixed_runtime_state_timestamp(
        self, payload: Dict[str, Any], prefix: str
    ) -> datetime:
        latest = datetime.min.replace(tzinfo=timezone.utc)
        for key, value in payload.items():
            if not key.startswith(f"{prefix}_"):
                continue
            if isinstance(value, (dict, list, tuple, set)):
                continue
            parsed = _parse_iso(value)
            if parsed and parsed > latest:
                latest = parsed

        if prefix == "history_sync":
            checkpoint = payload.get("history_sync_checkpoint")
            if isinstance(checkpoint, dict):
                parsed = _parse_iso(checkpoint.get("last_checkpoint_at"))
                if parsed and parsed > latest:
                    latest = parsed

        return latest

    def _overlay_prefixed_runtime_state(
        self, target: Dict[str, Any], source: Dict[str, Any], prefix: str
    ) -> Dict[str, Any]:
        updated = dict(target)
        for key, value in source.items():
            if key.startswith(f"{prefix}_"):
                updated[key] = value
        if prefix == "history_sync" and "history_sync_checkpoint" in source:
            updated["history_sync_checkpoint"] = source["history_sync_checkpoint"]
        return updated

    async def _contact_payload_from_user(
        self,
        user: Any,
        *,
        dialog: Any = None,
        sync_source: str,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(user, types.User):
            return None
        if getattr(user, "bot", False) or getattr(user, "is_self", False):
            return None
        user_id = getattr(user, "id", None)
        if user_id is None:
            return None

        avatar_url = await self._profile_photo_url(user)
        return {
            "peer_user_id": str(user_id),
            "chat_id": str(user_id),
            **self._serialize_user_profile(user),
            "avatar_url": avatar_url,
            "sync_source": sync_source,
            "last_message_at": (
                _serialize_datetime(getattr(dialog, "date", None)) if dialog else None
            ),
            "unread_count": (
                int(getattr(dialog, "unread_count", 0) or 0) if dialog else 0
            ),
        }

    def _should_sync_dialog(self, dialog: Any) -> bool:
        entity = getattr(dialog, "entity", None)
        if not getattr(dialog, "is_user", False):
            return False
        if isinstance(entity, types.User) and getattr(entity, "bot", False):
            return False
        if isinstance(entity, types.User) and getattr(entity, "is_self", False):
            return False
        return True

    async def _iter_history_payloads_for_dialog_user_id(
        self,
        dialog_user_id: int,
        *,
        cutoff: datetime | None,
        reset_cursor: bool,
        min_message_id: int = 0,
    ):
        entity = await self._resolve_entity(str(dialog_user_id))
        pending_album: list[Any] = []
        pending_grouped_id: Any = None

        async for message in self._client.iter_messages(
            entity,
            limit=None if reset_cursor else self._settings.history_message_limit,
            min_id=min_message_id,
            reverse=True,
        ):
            if not self._should_sync_history_message(message):
                continue
            if not _message_after(message, cutoff):
                continue
            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                if pending_grouped_id not in (None, grouped_id):
                    yield await self._serialize_history_album(pending_album)
                    pending_album = []
                pending_grouped_id = grouped_id
                pending_album.append(message)
                continue

            if pending_album:
                yield await self._serialize_history_album(pending_album)
                pending_album = []
                pending_grouped_id = None

            yield await self._serialize_message_event(message)

        if pending_album:
            yield await self._serialize_history_album(pending_album)

    async def _serialize_history_messages(
        self, messages: list[Any]
    ) -> list[Dict[str, Any]]:
        payloads: list[Dict[str, Any]] = []
        index = 0
        while index < len(messages):
            message = messages[index]
            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                grouped_messages = [message]
                index += 1
                while (
                    index < len(messages)
                    and getattr(messages[index], "grouped_id", None) == grouped_id
                ):
                    grouped_messages.append(messages[index])
                    index += 1
                payloads.append(await self._serialize_history_album(grouped_messages))
                continue

            payloads.append(await self._serialize_message_event(message))
            index += 1

        return payloads

    async def _serialize_history_album(self, messages: list[Any]) -> Dict[str, Any]:
        first_message = messages[0]
        sender = await first_message.get_sender()
        profile_user = await self._private_dialog_profile_user(
            first_message, sender=sender
        )
        peer_user_id = self._private_dialog_user_id(first_message, sender=sender)
        avatar_url = await self._profile_photo_url(profile_user or sender)
        reply_to = getattr(first_message.reply_to, "reply_to_msg_id", None)
        attachments: list[Dict[str, Any]] = []

        for message in messages:
            media_attachments, _, _ = await self._extract_media(message)
            attachments.extend(media_attachments)

        message_ids = [str(message.id) for message in messages if message.id]

        return {
            "message_id": first_message.id,
            "telegram_message_ids": message_ids,
            "grouped_id": (
                str(first_message.grouped_id) if first_message.grouped_id else None
            ),
            "message_created_at": _serialize_datetime(
                getattr(first_message, "date", None)
            ),
            "chat_id": str(peer_user_id) if peer_user_id else None,
            "peer_user_id": str(peer_user_id) if peer_user_id else None,
            "sender_id": str(getattr(sender, "id", None)) if sender else None,
            "chat_type": "private",
            "text": first_message.message or None,
            "caption": first_message.message or None,
            **self._serialize_user_profile(profile_user or sender),
            "outgoing_echo": any(bool(message.out) for message in messages),
            "reply_to_message_id": str(reply_to) if reply_to else None,
            "attachments": attachments,
            "location": None,
            "contact": None,
            "reactions": self._serialize_reactions(first_message),
            "forwarded_from": self._serialize_forwarded_from(first_message),
            "avatar_url": avatar_url,
        }

    def _history_payload_idempotency_key(self, payload: Dict[str, Any]) -> str:
        grouped_id = payload.get("grouped_id")
        if grouped_id:
            return f"album:{grouped_id}"
        return f"message:{payload['message_id']}"

    def _history_payload_cursor_id(self, payload: Dict[str, Any]) -> int:
        message_ids = [
            int(message_id)
            for message_id in payload.get("telegram_message_ids", []) or []
            if str(message_id).strip()
        ]
        if message_ids:
            return max(message_ids)
        return int(payload["message_id"])

    def _should_sync_history_message(self, message: Any) -> bool:
        peer_id = getattr(message, "peer_id", None)
        if not isinstance(peer_id, types.PeerUser):
            return False
        if getattr(message, "action", None) is not None:
            return False
        has_content = bool(getattr(message, "message", None))
        has_media = getattr(message, "media", None) is not None
        return has_content or has_media

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        if getattr(getattr(event, "message", None), "grouped_id", None):
            return
        await self._handle_message_event(event=event, event_name="message.created")

    async def _on_album(self, event: events.Album.Event) -> None:
        if not getattr(event, "is_private", False):
            return
        try:
            payload = await self._serialize_album_event(event)
            await self._callbacks.send_event(
                callback_url=self.state.callback_url,
                webhook_secret=self.state.webhook_secret,
                channel_id=self.state.channel_id,
                event="message.created",
                data=payload,
            )
            self.state.last_inbound_at = _iso_now()
        except Exception:
            logger.exception(
                "failed to serialize or deliver album for channel=%s",
                self.state.channel_id,
            )

    async def _on_message_edited(self, event: events.MessageEdited.Event) -> None:
        await self._handle_message_event(event=event, event_name="message.updated")

    async def _on_message_read(self, event: events.MessageRead.Event) -> None:
        if not getattr(event, "outbox", False):
            return
        if not getattr(event, "is_private", False):
            return
        try:
            chat_id = self._event_chat_user_id(event)
            if not chat_id or not event.max_id:
                return
            await self._callbacks.send_event(
                callback_url=self.state.callback_url,
                webhook_secret=self.state.webhook_secret,
                channel_id=self.state.channel_id,
                event="message.read",
                data={
                    "chat_id": str(chat_id),
                    "peer_user_id": str(chat_id),
                    "max_id": str(event.max_id),
                },
            )
        except Exception:
            logger.exception(
                "failed to deliver message.read for channel=%s",
                self.state.channel_id,
            )

    async def _on_message_reactions(self, update: types.UpdateMessageReactions) -> None:
        peer = getattr(update, "peer", None)
        if not isinstance(peer, types.PeerUser):
            return
        try:
            await self._callbacks.send_event(
                callback_url=self.state.callback_url,
                webhook_secret=self.state.webhook_secret,
                channel_id=self.state.channel_id,
                event="message.updated",
                data={
                    "message_id": str(update.msg_id),
                    "chat_id": str(peer.user_id),
                    "peer_user_id": str(peer.user_id),
                    "reactions": self._serialize_message_reactions(
                        getattr(update, "reactions", None), allow_empty=True
                    ),
                },
            )
            self.state.last_inbound_at = _iso_now()
        except Exception:
            logger.exception(
                "failed to deliver message.updated reaction snapshot for channel=%s",
                self.state.channel_id,
            )

    async def _on_message_deleted(self, event: events.MessageDeleted.Event) -> None:
        message_ids = [
            str(message_id)
            for message_id in getattr(event, "deleted_ids", []) or []
            if message_id
        ]
        if not message_ids:
            return
        try:
            await self._callbacks.send_event(
                callback_url=self.state.callback_url,
                webhook_secret=self.state.webhook_secret,
                channel_id=self.state.channel_id,
                event="message.deleted",
                data={"message_ids": message_ids},
            )
        except Exception:
            logger.exception(
                "failed to deliver message.deleted for channel=%s",
                self.state.channel_id,
            )

    async def _handle_message_event(
        self, *, event: events.common.EventCommon, event_name: str
    ) -> None:
        if not await self._is_private_user_message(event):
            return
        try:
            emitted_event = event_name
            if getattr(event.message, "action", None) is not None:
                if event_name != "message.created":
                    return
                payload = await self._serialize_activity_event(event.message)
                if payload is None:
                    return
                emitted_event = "message.activity"
            else:
                payload = await self._serialize_message_event(event.message)
            await self._callbacks.send_event(
                callback_url=self.state.callback_url,
                webhook_secret=self.state.webhook_secret,
                channel_id=self.state.channel_id,
                event=emitted_event,
                data=payload,
            )
            self.state.last_inbound_at = _iso_now()
        except Exception:
            logger.exception(
                "failed to serialize or deliver %s for channel=%s",
                event_name,
                self.state.channel_id,
            )

    async def _is_private_user_message(self, event: events.common.EventCommon) -> bool:
        if not getattr(event, "is_private", False):
            return False
        peer_id = getattr(getattr(event, "message", None), "peer_id", None)
        return isinstance(peer_id, types.PeerUser)

    def _event_chat_user_id(self, event: events.common.EventCommon) -> Optional[int]:
        chat_id = getattr(event, "chat_id", None)
        if chat_id:
            return int(chat_id)

        peer = getattr(event, "_chat_peer", None)
        if isinstance(peer, types.PeerUser):
            return int(peer.user_id)

        return None

    async def _serialize_activity_event(
        self, message: types.Message
    ) -> Optional[Dict[str, Any]]:
        activity_type = self._activity_type_for_message(message)
        if activity_type is None:
            return None

        sender = await message.get_sender()
        profile_user = await self._private_dialog_profile_user(message, sender=sender)
        peer_user_id = self._private_dialog_user_id(message, sender=sender)
        avatar_url = await self._profile_photo_url(profile_user or sender)

        return {
            "message_id": message.id,
            "message_created_at": _serialize_datetime(getattr(message, "date", None)),
            "chat_id": str(peer_user_id) if peer_user_id else None,
            "peer_user_id": str(peer_user_id) if peer_user_id else None,
            "sender_id": str(getattr(sender, "id", None)) if sender else None,
            "chat_type": "private",
            **self._serialize_user_profile(profile_user or sender),
            "outgoing_echo": bool(message.out),
            "activity_type": activity_type,
            "activity_params": self._activity_params_for_message(message),
            "avatar_url": avatar_url,
        }

    async def _serialize_message_event(self, message: types.Message) -> Dict[str, Any]:
        sender = await message.get_sender()
        profile_user = await self._private_dialog_profile_user(message, sender=sender)
        peer_user_id = self._private_dialog_user_id(message, sender=sender)
        attachments = []
        location = None
        contact = None
        if message.media:
            attachments, location, contact = await self._extract_media(message)

        avatar_url = await self._profile_photo_url(profile_user or sender)
        reply_to = getattr(message.reply_to, "reply_to_msg_id", None)
        return {
            "message_id": message.id,
            "message_created_at": _serialize_datetime(getattr(message, "date", None)),
            "chat_id": str(peer_user_id) if peer_user_id else None,
            "peer_user_id": str(peer_user_id) if peer_user_id else None,
            "sender_id": str(getattr(sender, "id", None)) if sender else None,
            "chat_type": "private",
            "text": message.message or None,
            "caption": message.message or None,
            **self._serialize_user_profile(profile_user or sender),
            "outgoing_echo": bool(message.out),
            "reply_to_message_id": str(reply_to) if reply_to else None,
            "attachments": attachments,
            "location": location,
            "contact": contact,
            "reactions": self._serialize_reactions(message),
            "forwarded_from": self._serialize_forwarded_from(message),
            "avatar_url": avatar_url,
        }

    def _serialize_user_profile(self, user: Any) -> Dict[str, Any]:
        if not isinstance(user, types.User):
            return {
                "first_name": getattr(user, "first_name", None),
                "last_name": getattr(user, "last_name", None),
                "username": getattr(user, "username", None),
                "language_code": getattr(user, "lang_code", None),
                "avatar_fingerprint": self._profile_photo_fingerprint(user),
            }

        phone_number = getattr(user, "phone", None)
        return {
            "first_name": getattr(user, "first_name", None),
            "last_name": getattr(user, "last_name", None),
            "username": getattr(user, "username", None),
            "language_code": getattr(user, "lang_code", None),
            "phone_number": str(phone_number) if phone_number else None,
            "is_bot": bool(getattr(user, "bot", False)),
            "is_contact": bool(getattr(user, "contact", False)),
            "is_mutual_contact": bool(getattr(user, "mutual_contact", False)),
            "is_premium": bool(getattr(user, "premium", False)),
            "is_verified": bool(getattr(user, "verified", False)),
            "is_scam": bool(getattr(user, "scam", False)),
            "is_fake": bool(getattr(user, "fake", False)),
            "is_restricted": bool(getattr(user, "restricted", False)),
            "is_deleted": bool(getattr(user, "deleted", False)),
            "avatar_fingerprint": self._profile_photo_fingerprint(user),
        }

    def _activity_type_for_message(self, message: types.Message) -> Optional[str]:
        action = getattr(message, "action", None)
        if isinstance(action, types.MessageActionContactSignUp):
            return "contact_joined"
        if isinstance(action, types.MessageActionHistoryClear):
            return "history_cleared"
        if isinstance(action, types.MessageActionPhoneCall):
            return self._phone_call_activity_type(action)
        return None

    def _activity_params_for_message(self, message: types.Message) -> Dict[str, Any]:
        action = getattr(message, "action", None)
        params: Dict[str, Any] = {}
        if isinstance(action, types.MessageActionPhoneCall):
            duration = getattr(action, "duration", None)
            if duration:
                params["duration_seconds"] = int(duration)
        return params

    def _phone_call_activity_type(self, action: types.MessageActionPhoneCall) -> str:
        call_suffix = "video_call" if getattr(action, "video", False) else "voice_call"
        discard_reason = getattr(action, "discard_reason", None)

        if isinstance(discard_reason, types.PhoneCallDiscardReasonMissed):
            prefix = "missed"
        elif isinstance(discard_reason, types.PhoneCallDiscardReasonBusy):
            prefix = "busy"
        else:
            prefix = "completed"

        return f"{prefix}_{call_suffix}"

    def _serialize_reactions(self, message: types.Message) -> Optional[Dict[str, Any]]:
        return self._serialize_message_reactions(getattr(message, "reactions", None))

    def _serialize_message_reactions(
        self, reactions: Any, allow_empty: bool = False
    ) -> Optional[Dict[str, Any]]:
        results = getattr(reactions, "results", None) or []
        if not results and not allow_empty:
            return None

        items = []
        total_count = 0
        for result in results:
            reaction = getattr(result, "reaction", None)
            serialized_reaction = self._serialize_reaction_value(reaction)
            if serialized_reaction is None:
                continue
            count = int(getattr(result, "count", 0) or 0)
            total_count += count
            item = {
                "reaction": serialized_reaction,
                "count": count,
            }
            chosen = getattr(result, "chosen_order", None)
            if chosen is not None:
                item["chosen_order"] = int(chosen)
            if getattr(result, "chosen", None) is not None:
                item["chosen"] = bool(getattr(result, "chosen"))
            items.append(item)

        recent_reactions = self._serialize_recent_reactions(
            getattr(reactions, "recent_reactions", None) or []
        )

        if not items and not allow_empty:
            return None

        payload = {
            "total_count": total_count,
            "results": items,
        }
        if recent_reactions:
            payload["recent_reactions"] = recent_reactions
        if getattr(reactions, "can_see_list", None) is not None:
            payload["can_see_list"] = bool(getattr(reactions, "can_see_list"))
        return payload

    def _serialize_recent_reactions(
        self, recent_reactions: list[Any]
    ) -> list[Dict[str, Any]]:
        items = []
        for item in recent_reactions:
            serialized_reaction = self._serialize_reaction_value(
                getattr(item, "reaction", None)
            )
            peer_id = self._serialize_peer_identifier(getattr(item, "peer_id", None))
            if serialized_reaction is None or peer_id is None:
                continue
            items.append(
                {
                    "peer_id": peer_id,
                    "reaction": serialized_reaction,
                    "date": _serialize_datetime(getattr(item, "date", None)),
                    "unread": bool(getattr(item, "unread", False)),
                    "my": bool(getattr(item, "my", False)),
                    "big": bool(getattr(item, "big", False)),
                }
            )
        return items

    def _serialize_reaction_value(self, reaction: Any) -> Optional[Dict[str, Any]]:
        if reaction is None:
            return None
        if isinstance(reaction, types.ReactionEmoji):
            return {"type": "emoji", "emoji": getattr(reaction, "emoticon", None)}
        if isinstance(reaction, types.ReactionCustomEmoji):
            return {
                "type": "custom_emoji",
                "document_id": str(getattr(reaction, "document_id", "")) or None,
            }
        if isinstance(reaction, types.ReactionPaid):
            return {"type": "paid"}
        return {"type": reaction.__class__.__name__}

    def _serialize_forwarded_from(
        self, message: types.Message
    ) -> Optional[Dict[str, Any]]:
        fwd_from = getattr(message, "fwd_from", None)
        if fwd_from is None:
            return None

        forwarded_from = {
            "from_id": self._serialize_peer_identifier(
                getattr(fwd_from, "from_id", None)
            ),
            "from_name": getattr(fwd_from, "from_name", None),
            "date": _serialize_datetime(getattr(fwd_from, "date", None)),
            "post_author": getattr(fwd_from, "post_author", None),
            "saved_from_peer": self._serialize_peer_identifier(
                getattr(fwd_from, "saved_from_peer", None)
            ),
            "saved_from_msg_id": (
                str(getattr(fwd_from, "saved_from_msg_id", None))
                if getattr(fwd_from, "saved_from_msg_id", None)
                else None
            ),
            "channel_post": (
                str(getattr(fwd_from, "channel_post", None))
                if getattr(fwd_from, "channel_post", None)
                else None
            ),
        }
        compact = {
            key: value for key, value in forwarded_from.items() if value is not None
        }
        return compact or None

    def _serialize_peer_identifier(self, peer: Any) -> Optional[Dict[str, Any]]:
        if isinstance(peer, types.PeerUser):
            return {"type": "user", "id": str(peer.user_id)}
        if isinstance(peer, types.PeerChannel):
            return {"type": "channel", "id": str(peer.channel_id)}
        if isinstance(peer, types.PeerChat):
            return {"type": "chat", "id": str(peer.chat_id)}
        return None

    async def _serialize_album_event(self, event: events.Album.Event) -> Dict[str, Any]:
        first_message = event.messages[0]
        sender = await first_message.get_sender()
        profile_user = await self._private_dialog_profile_user(
            first_message, sender=sender
        )
        peer_user_id = self._private_dialog_user_id(first_message, sender=sender)
        avatar_url = await self._profile_photo_url(profile_user or sender)
        reply_to = getattr(first_message.reply_to, "reply_to_msg_id", None)
        attachments: list[Dict[str, Any]] = []

        for message in event.messages:
            media_attachments, _, _ = await self._extract_media(message)
            attachments.extend(media_attachments)

        message_ids = [str(message.id) for message in event.messages if message.id]

        return {
            "message_id": first_message.id,
            "telegram_message_ids": message_ids,
            "grouped_id": str(event.grouped_id) if event.grouped_id else None,
            "message_created_at": _serialize_datetime(
                getattr(first_message, "date", None)
            ),
            "chat_id": str(peer_user_id) if peer_user_id else None,
            "peer_user_id": str(peer_user_id) if peer_user_id else None,
            "sender_id": str(getattr(sender, "id", None)) if sender else None,
            "chat_type": "private",
            "text": event.text or None,
            "caption": event.text or None,
            **self._serialize_user_profile(profile_user or sender),
            "outgoing_echo": any(bool(message.out) for message in event.messages),
            "reply_to_message_id": str(reply_to) if reply_to else None,
            "attachments": attachments,
            "location": None,
            "contact": None,
            "reactions": self._serialize_reactions(first_message),
            "forwarded_from": self._serialize_forwarded_from(first_message),
            "avatar_url": avatar_url,
        }

    def _private_dialog_user_id(
        self, message: Any, *, sender: Any = None
    ) -> Optional[int]:
        peer = getattr(message, "peer_id", None)
        if isinstance(peer, types.PeerUser):
            return int(peer.user_id)

        sender_id = getattr(sender, "id", None)
        if sender_id:
            return int(sender_id)

        return None

    async def _private_dialog_profile_user(
        self, message: Any, *, sender: Any = None
    ) -> Any:
        peer_user_id = self._private_dialog_user_id(message, sender=sender)
        if peer_user_id is None:
            return sender

        if (
            isinstance(sender, types.User)
            and getattr(sender, "id", None) == peer_user_id
            and not getattr(sender, "is_self", False)
        ):
            return sender

        get_chat = getattr(message, "get_chat", None)
        if callable(get_chat):
            try:
                chat = await get_chat()
                if isinstance(chat, types.User):
                    return chat
            except Exception:
                logger.debug(
                    "failed to resolve private dialog chat entity for channel=%s peer_user_id=%s",
                    self.state.channel_id,
                    peer_user_id,
                )

        if (
            isinstance(sender, types.User)
            and getattr(sender, "id", None) == peer_user_id
        ):
            return sender

        try:
            entity = await self._client.get_entity(peer_user_id)
            if isinstance(entity, types.User):
                return entity
        except Exception:
            logger.debug(
                "failed to resolve private dialog peer entity for channel=%s peer_user_id=%s",
                self.state.channel_id,
                peer_user_id,
            )

        return sender

    async def _extract_media(
        self, message: types.Message
    ) -> tuple[
        list[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]
    ]:
        media = message.media
        attachments: list[Dict[str, Any]] = []
        location = None
        contact = None

        if isinstance(media, types.MessageMediaGeo):
            location = {
                "latitude": media.geo.lat,
                "longitude": media.geo.long,
            }
        elif isinstance(media, types.MessageMediaVenue):
            location = {
                "latitude": media.geo.lat,
                "longitude": media.geo.long,
                "name": getattr(media, "title", None)
                or getattr(media, "address", None),
                "address": getattr(media, "address", None),
                "provider": getattr(media, "provider", None),
                "venue_id": getattr(media, "venue_id", None),
            }
        elif isinstance(media, types.MessageMediaGeoLive):
            location = {
                "latitude": media.geo.lat,
                "longitude": media.geo.long,
            }
        elif isinstance(media, types.MessageMediaContact):
            contact = {
                "phone_number": media.phone_number,
                "first_name": media.first_name,
                "last_name": media.last_name,
            }
        else:
            source_path = await self._client.download_media(
                message,
                file=str(Path(tempfile.gettempdir()) / f"telegram-media-{uuid4_hex()}"),
            )
            if source_path:
                try:
                    kind, filename, content_type = _media_kind_and_name(media)
                    stored = self._media_store.store_path(
                        source_path=source_path,
                        filename=filename,
                        content_type=content_type,
                    )
                    attachments.append(
                        {
                            "kind": kind,
                            "url": self._media_store.signed_url(stored.media_id),
                            "filename": stored.filename,
                            "content_type": stored.content_type,
                        }
                    )
                finally:
                    Path(source_path).unlink(missing_ok=True)

        return attachments, location, contact

    async def _profile_photo_url(self, sender: Any) -> Optional[str]:
        if sender is None:
            return None
        cache_key = self._profile_photo_cache_key(sender)
        if cache_key and cache_key in self._profile_photo_url_cache:
            return self._profile_photo_url_cache[cache_key]
        avatar_stored, stored = await self._download_profile_photo_assets(
            sender,
            avatar_fingerprint=self._profile_photo_fingerprint(sender),
        )
        del avatar_stored
        if stored is None:
            self._remember_profile_photo_url(sender, None)
            return None

        signed_url = self._media_store.signed_url(stored.media_id)
        self._remember_profile_photo_url(sender, signed_url)
        return signed_url

    async def _download_profile_photo_assets(
        self,
        sender: Any,
        *,
        avatar_fingerprint: Optional[str],
        store_transient_media: bool = True,
    ):
        peer_user_id = getattr(sender, "id", None)
        profile_path = await self._client.download_profile_photo(
            sender,
            file=str(Path(tempfile.gettempdir()) / f"telegram-avatar-{uuid4_hex()}"),
            download_big=False,
        )
        if not profile_path:
            return None, None

        avatar_stored = None
        transient_stored = None
        try:
            content_type = mimetypes.guess_type(str(profile_path))[0] or "image/jpeg"
            if avatar_fingerprint and peer_user_id is not None:
                avatar_stored = self._media_store.store_profile_avatar_path(
                    channel_id=self.state.channel_id,
                    peer_user_id=str(peer_user_id),
                    avatar_fingerprint=avatar_fingerprint,
                    source_path=profile_path,
                    filename=Path(profile_path).name,
                    content_type=content_type,
                )
            if store_transient_media:
                transient_stored = self._media_store.store_path(
                    source_path=profile_path,
                    filename=Path(profile_path).name,
                    content_type=content_type,
                )
            return avatar_stored, transient_stored
        finally:
            Path(profile_path).unlink(missing_ok=True)

    def _remember_profile_photo_url(self, sender: Any, url: Optional[str]) -> None:
        cache_key = self._profile_photo_cache_key(sender)
        cache_prefix = self._profile_photo_cache_prefix(sender)
        if cache_key is None or cache_prefix is None:
            return

        stale_keys = [
            key
            for key in self._profile_photo_url_cache
            if key.startswith(cache_prefix) and key != cache_key
        ]
        for key in stale_keys:
            self._profile_photo_url_cache.pop(key, None)
        self._profile_photo_url_cache[cache_key] = url

    def _profile_photo_cache_key(self, sender: Any) -> Optional[str]:
        cache_prefix = self._profile_photo_cache_prefix(sender)
        if cache_prefix is None:
            return None

        avatar_fingerprint = self._profile_photo_fingerprint(sender) or "none"
        return f"{cache_prefix}{avatar_fingerprint}"

    def _profile_photo_cache_prefix(self, sender: Any) -> Optional[str]:
        sender_id = getattr(sender, "id", None)
        if sender_id is None:
            return None
        return f"{sender.__class__.__name__}:{sender_id}:"

    def _profile_photo_fingerprint(self, sender: Any) -> Optional[str]:
        photo = getattr(sender, "photo", None)
        photo_id = getattr(photo, "photo_id", None)
        if photo_id is None:
            return None
        return str(photo_id)

    async def _download_attachment(self, attachment: Any) -> str:
        url = str(getattr(attachment, "url", ""))
        response = await self._http_client.get(url)
        response.raise_for_status()
        target = (
            Path(tempfile.gettempdir())
            / f"telegram-outbound-{uuid4_hex()}{self._attachment_suffix(attachment)}"
        )
        target.write_bytes(response.content)
        return str(target)

    def _attachment_suffix(self, attachment: Any) -> str:
        filename = getattr(attachment, "filename", None)
        if filename:
            suffix = Path(filename).suffix
            if suffix:
                return suffix

        url = str(getattr(attachment, "url", ""))
        remote_suffix = Path(urlparse(url).path).suffix
        if remote_suffix:
            return remote_suffix

        content_type = (
            str(getattr(attachment, "content_type", "") or "").split(";")[0].strip()
        )
        if content_type:
            guessed_suffix = mimetypes.guess_extension(content_type)
            if guessed_suffix:
                return guessed_suffix

        return {
            "image": ".jpg",
            "audio": ".mp3",
            "video": ".mp4",
        }.get(getattr(attachment, "file_type", None), "")

    def _outbound_media_kwargs(self, attachment: Any) -> dict[str, Any]:
        file_type = getattr(attachment, "file_type", None)
        content_type = (
            str(getattr(attachment, "content_type", "") or "").split(";")[0].strip()
        )
        voice_note = bool(getattr(attachment, "voice_note", False))

        kwargs: dict[str, Any] = {}
        if file_type == "file" and not voice_note:
            kwargs["force_document"] = True
        if file_type == "video":
            kwargs["supports_streaming"] = True
        if voice_note:
            kwargs["voice_note"] = True
        if content_type:
            kwargs["mime_type"] = content_type

        return kwargs

    async def _private_unread_reactions_count(self) -> int:
        total = 0
        async for dialog in self._client.iter_dialogs(
            limit=self._settings.history_dialog_limit,
            ignore_migrated=True,
            archived=False,
        ):
            if not self._should_sync_dialog(dialog):
                continue
            total += int(getattr(dialog, "unread_reactions_count", 0) or 0)
        return total

    async def _resolve_entity(self, raw: str):
        rid = (raw or "").strip()
        if not rid:
            raise ValueError("recipient_id is empty")
        if rid.startswith("@"):
            return rid
        if rid.isdigit():
            return await self._client.get_entity(int(rid))
        return await self._client.get_entity(rid)

    def _export_string_session(self) -> str:
        session = self._client.session
        if isinstance(session, StringSession):
            return session.save()
        return StringSession.save(session)

    def _can_send_as_album(self, attachments: list[Any]) -> bool:
        if len(attachments) < 2:
            return False

        allowed_types = {"image", "video"}
        return all(
            getattr(attachment, "file_type", None) in allowed_types
            for attachment in attachments
        )

    async def _edit_existing_message(
        self,
        *,
        entity: Any,
        message_id: int,
        text: str,
    ):
        existing_message = await self._client.get_messages(entity, ids=message_id)
        if isinstance(existing_message, list):
            existing_message = existing_message[0] if existing_message else None

        if getattr(existing_message, "edit", None):
            try:
                # Prefer editing the fetched message object so Telethon preserves
                # the original preview/reply-markup state for user messages.
                return await existing_message.edit(text, parse_mode=None)
            except errors.MessageNotModifiedError:
                return existing_message

        try:
            return await self._client.edit_message(
                entity,
                message=message_id,
                text=text,
                parse_mode=None,
            )
        except errors.MessageNotModifiedError:
            return existing_message


class RuntimeManager:
    def __init__(self, settings: GatewaySettings):
        self._settings = settings
        self._callbacks = CallbackClient(settings)
        self._media_store = MediaStore(settings)
        self._store = GatewayStore(settings.state_db_path)
        self._store.prune_finished_events(
            older_than_hours=settings.outbox_retention_hours
        )
        self._runtimes: Dict[int, ChannelRuntime] = {}

    @property
    def media_store(self) -> MediaStore:
        return self._media_store

    async def shutdown(self) -> None:
        for runtime in self._runtimes.values():
            await runtime.close()
        await self._callbacks.close()
        self._store.close()

    def _runtime(self, channel_id: int) -> ChannelRuntime:
        runtime = self._runtimes.get(channel_id)
        if runtime is None:
            raise KeyError(f"Channel {channel_id} is not synced")
        return runtime

    async def sync(
        self, channel_id: int, payload: ChannelSyncPayload
    ) -> Dict[str, Any]:
        runtime = self._runtimes.get(channel_id)
        if runtime is None:
            persisted_runtime_state = self._store.load_runtime_state(channel_id)
            runtime = ChannelRuntime(
                state=RuntimeState(
                    channel_id=channel_id,
                    api_id=payload.api_id,
                    api_hash=payload.api_hash,
                    phone_number=payload.phone_number,
                    string_session=payload.string_session or "",
                    callback_url=str(payload.callback_url),
                    webhook_secret=payload.webhook_secret,
                    runtime_state=dict(
                        persisted_runtime_state or payload.runtime_state or {}
                    ),
                ),
                settings=self._settings,
                callbacks=self._callbacks,
                media_store=self._media_store,
                store=self._store,
            )
            self._runtimes[channel_id] = runtime
        snapshot = await runtime.sync(payload)
        return {"channel": snapshot}

    async def request_login_code(self, channel_id: int) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).request_login_code()}

    async def request_qr_login(self, channel_id: int) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).request_qr_login()}

    async def verify_code(self, channel_id: int, code: str) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).verify_code(code)}

    async def verify_password(self, channel_id: int, password: str) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).verify_password(password)}

    async def reconnect(self, channel_id: int) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).reconnect()}

    async def history_sync(
        self,
        channel_id: int,
        force: bool = True,
        reset_cursor: bool = False,
        include_contacts: bool = False,
    ) -> Dict[str, Any]:
        return {
            "channel": await self._runtime(channel_id).history_sync(
                force=force,
                reset_cursor=reset_cursor,
                include_contacts=include_contacts,
            )
        }

    async def contacts_sync(
        self, channel_id: int, force: bool = True
    ) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).contacts_sync(force=force)}

    async def disconnect(self, channel_id: int) -> Dict[str, Any]:
        return {"channel": await self._runtime(channel_id).disconnect()}

    async def teardown(self, channel_id: int) -> Dict[str, Any]:
        runtime = self._runtimes.pop(channel_id, None)
        if runtime is not None:
            await runtime.close()
        else:
            Path(f"/tmp/telegram-personal-{channel_id}").unlink(missing_ok=True)
        return {"ok": True}

    async def diagnostics(self, channel_id: int) -> Dict[str, Any]:
        return await self._runtime(channel_id).diagnostics()

    async def fetch_profile_avatar(
        self, channel_id: int, *, peer_user_id: str, avatar_fingerprint: str
    ):
        try:
            return self._media_store.get_profile_avatar(
                channel_id=channel_id,
                peer_user_id=peer_user_id,
                avatar_fingerprint=avatar_fingerprint,
            )
        except FileNotFoundError:
            runtime = self._runtimes.get(channel_id)
            if runtime is None:
                raise KeyError(f"Channel {channel_id} is not synced")
            return await runtime.fetch_profile_avatar(
                peer_user_id=peer_user_id,
                avatar_fingerprint=avatar_fingerprint,
            )

    async def send_message(
        self, channel_id: int, payload: SendMessagePayload
    ) -> Dict[str, Any]:
        return await self._runtime(channel_id).send_message(payload)

    async def edit_message(
        self, channel_id: int, payload: EditMessagePayload
    ) -> Dict[str, Any]:
        return await self._runtime(channel_id).edit_message(payload)

    async def delete_messages(
        self, channel_id: int, payload: DeleteMessagesPayload
    ) -> Dict[str, Any]:
        return await self._runtime(channel_id).delete_messages(payload)

    async def mark_read(
        self, channel_id: int, payload: MarkReadPayload
    ) -> Dict[str, Any]:
        return await self._runtime(channel_id).mark_read(payload)


def _media_kind_and_name(media: Any) -> tuple[str, str, str]:
    if isinstance(media, types.MessageMediaPhoto):
        return "photo", "telegram-photo.jpg", "image/jpeg"
    if isinstance(media, types.MessageMediaDocument):
        document = media.document
        filename = "telegram-file"
        kind = "document"
        content_type = document.mime_type or "application/octet-stream"
        for attribute in document.attributes:
            if isinstance(attribute, types.DocumentAttributeFilename):
                filename = attribute.file_name
            elif isinstance(attribute, types.DocumentAttributeAudio):
                kind = "voice" if attribute.voice else "audio"
            elif isinstance(attribute, types.DocumentAttributeVideo):
                kind = "video"
            elif isinstance(attribute, types.DocumentAttributeSticker):
                kind = "sticker"
        if content_type.startswith("image/") and kind == "document":
            kind = "photo"
        return kind, filename, content_type
    return "document", "telegram-file.bin", "application/octet-stream"


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _iso_from_delta(seconds: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def _seconds_until(timestamp: Optional[str]) -> int:
    if not timestamp:
        return 0
    try:
        target = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ"))
    except ValueError:
        return 0
    return max(0, int(target - time.time()))


def _parse_iso(timestamp: Optional[Any]) -> Optional[datetime]:
    if not timestamp:
        return None
    if isinstance(timestamp, datetime):
        return (
            timestamp
            if timestamp.tzinfo is not None
            else timestamp.replace(tzinfo=timezone.utc)
        )
    if not isinstance(timestamp, str):
        return None
    try:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


def _message_after(message: Any, cutoff: Optional[datetime]) -> bool:
    if cutoff is None:
        return True
    message_date = getattr(message, "date", None)
    if message_date is None:
        return False
    if message_date.tzinfo is None:
        message_date = message_date.replace(tzinfo=timezone.utc)
    return message_date >= cutoff


def _serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _humanize_telegram_error(error: Exception) -> str:
    if isinstance(error, errors.PhoneCodeInvalidError):
        return "Invalid Telegram login code."
    if isinstance(error, errors.PhoneCodeExpiredError):
        return "Telegram login code expired. Request a new one."
    if isinstance(error, errors.PhoneCodeEmptyError):
        return "Telegram login code is required."
    if isinstance(error, errors.PhoneCodeHashEmptyError):
        return "Request a fresh Telegram login code before verifying."
    if isinstance(error, errors.PasswordHashInvalidError):
        return "Invalid Telegram 2FA password."
    if isinstance(error, errors.SendCodeUnavailableError):
        return (
            "Telegram already used all available code delivery options. "
            "Use the last code you received or wait before requesting a new one."
        )
    if isinstance(error, errors.FloodWaitError):
        return (
            f"Telegram rate limit reached. Wait {error.seconds} seconds and try again."
        )
    return str(error)


def uuid4_hex() -> str:
    return next(tempfile._get_candidate_names())
