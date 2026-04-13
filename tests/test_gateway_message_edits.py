import tempfile
import unittest
from pathlib import Path

from telethon import errors

from app.config import GatewaySettings
from app.media_store import MediaStore
from app.outbox import GatewayStore
from app.runtime import ChannelRuntime, RuntimeState
from app.schemas import EditMessagePayload


class _FakeCallbacks:
    async def send_event(self, **_kwargs):
        return None

    async def close(self):
        return None


class _FakeEditableMessage:
    def __init__(self, message_id: int, *, error: Exception | None = None):
        self.id = message_id
        self.error = error
        self.edits = []

    async def edit(self, text: str, **kwargs):
        self.edits.append((text, kwargs))
        if self.error is not None:
            raise self.error
        return self


class _FakeClient:
    def __init__(self, *, fetched_message=None, edited_message=None):
        self.fetched_message = fetched_message
        self.edited_message = edited_message
        self.get_messages_calls = []
        self.edit_message_calls = []
        self.disconnect_calls = 0

    def is_connected(self):
        return False

    async def disconnect(self):
        self.disconnect_calls += 1
        return None

    async def get_messages(self, entity, ids):
        self.get_messages_calls.append((entity, ids))
        return self.fetched_message

    async def edit_message(self, entity, *, message, text, parse_mode):
        self.edit_message_calls.append(
            {
                "entity": entity,
                "message": message,
                "text": text,
                "parse_mode": parse_mode,
            }
        )
        return self.edited_message


class GatewayMessageEditTest(unittest.IsolatedAsyncioTestCase):
    def build_settings(self, root: Path) -> GatewaySettings:
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
        )

    async def build_runtime(self, fake_client: _FakeClient):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        root = Path(temp_dir.name)
        settings = self.build_settings(root)
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
            ),
            settings=settings,
            callbacks=_FakeCallbacks(),
            media_store=MediaStore(settings),
            store=store,
        )
        runtime._client = fake_client

        async def _noop():
            return None

        async def _resolve_entity(_raw: str):
            return "dialog-entity"

        runtime._ensure_client = _noop
        runtime._connect_if_needed = _noop
        runtime._emit_runtime_update = _noop
        runtime._resolve_entity = _resolve_entity
        return runtime

    async def test_edit_message_prefers_message_object_edit(self):
        fetched_message = _FakeEditableMessage(777)
        runtime = await self.build_runtime(
            _FakeClient(fetched_message=fetched_message, edited_message=None)
        )
        self.addAsyncCleanup(runtime.close)

        result = await runtime.edit_message(
            EditMessagePayload(
                recipient_id="134527512",
                chat_id="134527512",
                message_id="777",
                text="updated text",
            )
        )

        self.assertEqual("777", result["message_id"])
        self.assertEqual([("dialog-entity", 777)], runtime._client.get_messages_calls)
        self.assertEqual(
            [("updated text", {"parse_mode": None})],
            fetched_message.edits,
        )
        self.assertEqual([], runtime._client.edit_message_calls)

    async def test_edit_message_falls_back_to_client_edit_when_message_fetch_fails(self):
        edited_message = _FakeEditableMessage(778)
        runtime = await self.build_runtime(
            _FakeClient(fetched_message=None, edited_message=edited_message)
        )
        self.addAsyncCleanup(runtime.close)

        result = await runtime.edit_message(
            EditMessagePayload(
                recipient_id="134527512",
                chat_id="134527512",
                message_id="778",
                text="fallback text",
            )
        )

        self.assertEqual("778", result["message_id"])
        self.assertEqual(
            [
                {
                    "entity": "dialog-entity",
                    "message": 778,
                    "text": "fallback text",
                    "parse_mode": None,
                }
            ],
            runtime._client.edit_message_calls,
        )

    async def test_edit_message_treats_not_modified_as_success(self):
        fetched_message = _FakeEditableMessage(
            779,
            error=errors.MessageNotModifiedError(request=None),
        )
        runtime = await self.build_runtime(
            _FakeClient(fetched_message=fetched_message, edited_message=None)
        )
        self.addAsyncCleanup(runtime.close)

        result = await runtime.edit_message(
            EditMessagePayload(
                recipient_id="134527512",
                chat_id="134527512",
                message_id="779",
                text="same text",
            )
        )

        self.assertEqual("779", result["message_id"])
        self.assertEqual([], runtime._client.edit_message_calls)
