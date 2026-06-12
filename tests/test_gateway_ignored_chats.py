import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from telethon.tl import types

from app.config import GatewaySettings, parse_ignored_chat_ids
from app.media_store import MediaStore
from app.outbox import GatewayStore
from app.runtime import (
    CONTACTS_OUTBOX_CATEGORY,
    ChannelRuntime,
    RuntimeManager,
    RuntimeState,
)
from app.schemas import (
    DeleteMessagesPayload,
    EditMessagePayload,
    MarkReadPayload,
    SendMessagePayload,
)


class _FakeCallbacks:
    def __init__(self):
        self.deliveries = []

    async def send_event(self, **kwargs):
        self.deliveries.append(kwargs)

    async def close(self):
        return None


class _FakeClient:
    def __init__(self):
        self.resolve_calls = []

    def is_connected(self):
        return False

    async def disconnect(self):
        return None

    async def get_entity(self, raw):
        self.resolve_calls.append(raw)
        return raw


class _FakeMessage:
    def __init__(self, *, peer_user_id: int, sender_id: int | None = None):
        self.id = 101
        self.peer_id = types.PeerUser(peer_user_id)
        self.from_id = types.PeerUser(sender_id or peer_user_id)
        self.sender_id = sender_id or peer_user_id
        self.out = False
        self.message = "ignored"
        self.media = None
        self.action = None
        self.reply_to = None
        self.date = datetime(2026, 6, 12, 7, 50, tzinfo=timezone.utc)


class _FakeEvent:
    def __init__(self, *, chat_id: int):
        self.chat_id = chat_id
        self.is_private = True
        self.message = _FakeMessage(peer_user_id=chat_id)


class _FakeReadEvent:
    def __init__(self, *, chat_id: int):
        self.chat_id = chat_id
        self.is_private = True
        self.outbox = True
        self.max_id = 101


class _FakeDeleteEvent:
    def __init__(self, *, chat_id=None):
        self.chat_id = chat_id
        self.deleted_ids = [101]


class _FakeDialog:
    def __init__(self, *, user_id: int):
        self.is_user = True
        self.entity = types.User(id=user_id, access_hash=0, first_name="Test")
        self.peer_id = types.PeerUser(user_id)


class GatewayIgnoredChatsTest(unittest.IsolatedAsyncioTestCase):
    def build_settings(self, root: Path, *, ignored_chat_ids=None) -> GatewaySettings:
        return GatewaySettings(
            internal_token="token",
            public_base_url="http://127.0.0.1:8000",
            media_secret="media-secret",
            media_ttl_seconds=900,
            state_db_path=root / "state.sqlite3",
            callback_timeout_seconds=1,
            callback_max_retries=1,
            outbox_batch_size=10,
            outbox_poll_interval_seconds=0.01,
            outbox_max_delivery_attempts=3,
            outbox_retention_hours=168,
            host="127.0.0.1",
            port=8000,
            media_dir=root / "media",
            history_dialog_limit=20,
            history_message_limit=50,
            history_lookback_hours=24,
            history_overlap_seconds=300,
            contacts_dialog_limit=0,
            contacts_include_saved=True,
            contacts_include_dialogs=True,
            ignored_chat_ids=parse_ignored_chat_ids(ignored_chat_ids),
        )

    async def build_runtime(self, *, ignored_chat_ids=None, runtime_state=None):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        settings = self.build_settings(root, ignored_chat_ids=ignored_chat_ids)
        callbacks = _FakeCallbacks()
        store = GatewayStore(settings.state_db_path)
        self.addCleanup(store.close)
        runtime = ChannelRuntime(
            state=RuntimeState(
                channel_id=42,
                api_id=1,
                api_hash="hash",
                phone_number="+77000000000",
                callback_url="http://app.test/webhook",
                webhook_secret="secret",
                runtime_state=runtime_state or {},
            ),
            settings=settings,
            callbacks=callbacks,
            media_store=MediaStore(settings),
            store=store,
        )
        runtime._client = _FakeClient()

        async def _noop():
            return None

        runtime._ensure_client = _noop
        runtime._connect_if_needed = _noop
        runtime._emit_runtime_update = _noop
        return runtime, callbacks

    async def test_message_event_from_ignored_chat_is_not_serialized_or_delivered(self):
        runtime, callbacks = await self.build_runtime(ignored_chat_ids="5110864359")
        self.addAsyncCleanup(runtime.close)
        serialize_calls = []

        async def _serialize_message_event(_message):
            serialize_calls.append(True)
            return {"chat_id": "5110864359"}

        runtime._serialize_message_event = _serialize_message_event

        await runtime._handle_message_event(
            event=_FakeEvent(chat_id=5110864359),
            event_name="message.created",
        )

        self.assertEqual([], serialize_calls)
        self.assertEqual([], callbacks.deliveries)

    async def test_runtime_state_ignored_chat_ids_are_applied(self):
        runtime, callbacks = await self.build_runtime(
            runtime_state={"ignored_chat_ids": ["7001"]}
        )
        self.addAsyncCleanup(runtime.close)

        await runtime._handle_message_event(
            event=_FakeEvent(chat_id=7001),
            event_name="message.updated",
        )

        self.assertEqual([], callbacks.deliveries)

    async def test_runtime_state_ignored_chat_ids_are_replaced_on_sync(self):
        runtime, _callbacks = await self.build_runtime(
            runtime_state={"ignored_chat_ids": ["7001"]}
        )
        self.addAsyncCleanup(runtime.close)

        merged = runtime._merge_runtime_state({"ignored_chat_ids": ["8002"]})

        self.assertEqual(["8002"], merged["ignored_chat_ids"])

    async def test_read_and_reaction_events_from_ignored_chat_are_not_delivered(self):
        runtime, callbacks = await self.build_runtime(ignored_chat_ids="7001")
        self.addAsyncCleanup(runtime.close)

        await runtime._on_message_read(_FakeReadEvent(chat_id=7001))
        await runtime._on_message_reactions(
            type(
                "ReactionUpdate",
                (),
                {"peer": types.PeerUser(7001), "msg_id": 101, "reactions": None},
            )()
        )

        self.assertEqual([], callbacks.deliveries)

    async def test_delete_event_without_chat_context_is_not_delivered(self):
        runtime, callbacks = await self.build_runtime()
        self.addAsyncCleanup(runtime.close)

        await runtime._on_message_deleted(_FakeDeleteEvent())

        self.assertEqual([], callbacks.deliveries)

    async def test_contacts_and_history_skip_ignored_chats(self):
        runtime, _callbacks = await self.build_runtime(ignored_chat_ids="7001")
        self.addAsyncCleanup(runtime.close)

        self.assertFalse(runtime._should_sync_dialog(_FakeDialog(user_id=7001)))
        self.assertIsNone(
            await runtime._contact_payload_from_user(
                types.User(id=7001, access_hash=0, first_name="Ignored"),
                sync_source="saved_contact",
            )
        )
        payloads = [
            payload
            async for payload in runtime._iter_history_payloads_for_dialog_user_id(
                7001,
                cutoff=None,
                reset_cursor=True,
            )
        ]
        self.assertEqual([], payloads)
        self.assertEqual([], runtime._client.resolve_calls)

    async def test_persisted_import_event_for_ignored_chat_is_acked_without_delivery(
        self,
    ):
        runtime, callbacks = await self.build_runtime(ignored_chat_ids="7001")
        self.addAsyncCleanup(runtime.close)

        inserted = runtime._store.enqueue_event(
            channel_id=runtime.state.channel_id,
            event="contact.imported",
            category=CONTACTS_OUTBOX_CATEGORY,
            scope_key="contacts-run-1",
            idempotency_key="contact:7001",
            callback_url=runtime.state.callback_url,
            webhook_secret=runtime.state.webhook_secret,
            payload={"chat_id": "7001", "peer_user_id": "7001"},
        )
        self.assertTrue(inserted)

        event = runtime._store.due_events(
            channel_id=runtime.state.channel_id,
            limit=1,
        )[0]
        await runtime._deliver_outbox_event(event)

        self.assertEqual([], callbacks.deliveries)
        self.assertEqual(
            1,
            runtime._store.count_events(
                channel_id=runtime.state.channel_id,
                category=CONTACTS_OUTBOX_CATEGORY,
                scope_key="contacts-run-1",
                status="acked",
            ),
        )

    async def test_outbound_to_ignored_chat_is_rejected_before_resolve(self):
        runtime, _callbacks = await self.build_runtime(ignored_chat_ids="7001")
        self.addAsyncCleanup(runtime.close)

        with self.assertRaisesRegex(ValueError, "ignored by system policy"):
            await runtime.send_message(
                SendMessagePayload(
                    recipient_id="7001",
                    chat_id="7001",
                    text="hello",
                )
            )

        self.assertEqual([], runtime._client.resolve_calls)

    async def test_other_outbound_actions_to_ignored_chat_are_rejected_before_resolve(
        self,
    ):
        runtime, _callbacks = await self.build_runtime(ignored_chat_ids="7001")
        self.addAsyncCleanup(runtime.close)

        with self.assertRaisesRegex(ValueError, "ignored by system policy"):
            await runtime.mark_read(
                MarkReadPayload(recipient_id="7001", chat_id="7001", max_id="10")
            )
        with self.assertRaisesRegex(ValueError, "ignored by system policy"):
            await runtime.edit_message(
                EditMessagePayload(
                    recipient_id="7001",
                    chat_id="7001",
                    message_id="10",
                    text="edited",
                )
            )
        with self.assertRaisesRegex(ValueError, "ignored by system policy"):
            await runtime.delete_messages(
                DeleteMessagesPayload(
                    recipient_id="7001",
                    chat_id="7001",
                    message_ids=["10"],
                )
            )

        self.assertEqual([], runtime._client.resolve_calls)

    async def test_manager_rejects_cached_avatar_for_ignored_chat(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root, ignored_chat_ids="7001")
            manager = RuntimeManager(settings)
            avatar_path = root / "avatar.jpg"
            avatar_path.write_bytes(b"avatar")
            manager.media_store.store_profile_avatar_path(
                channel_id=42,
                peer_user_id="7001",
                avatar_fingerprint="photo-1",
                source_path=avatar_path,
                filename="avatar.jpg",
                content_type="image/jpeg",
            )

            try:
                with self.assertRaises(FileNotFoundError):
                    await manager.fetch_profile_avatar(
                        42,
                        peer_user_id="7001",
                        avatar_fingerprint="photo-1",
                    )
            finally:
                await manager.shutdown()
