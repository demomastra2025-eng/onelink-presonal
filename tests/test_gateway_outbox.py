import asyncio
import tempfile
import unittest
from pathlib import Path

from app.config import GatewaySettings
from app.media_store import MediaStore
from app.outbox import GatewayStore
from app.runtime import (
    HISTORY_MESSAGE_EVENT,
    HISTORY_OUTBOX_CATEGORY,
    ChannelRuntime,
    RuntimeState,
)


class _FakeCallbacks:
    def __init__(self, failures_before_success: int = 0):
        self.failures_before_success = failures_before_success
        self.deliveries = []

    async def send_event(self, *, callback_url, webhook_secret, channel_id, event, data):
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise RuntimeError("callback unavailable")
        self.deliveries.append(
            {
                "callback_url": callback_url,
                "webhook_secret": webhook_secret,
                "channel_id": channel_id,
                "event": event,
                "data": data,
            }
        )

    async def close(self):
        return None


class GatewayOutboxTest(unittest.IsolatedAsyncioTestCase):
    def build_settings(self, root: Path, *, max_attempts: int = 3) -> GatewaySettings:
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
            outbox_max_delivery_attempts=max_attempts,
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

    async def test_history_event_is_delivered_from_outbox_and_marks_sync_completed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root)
            callbacks = _FakeCallbacks()
            store = GatewayStore(settings.state_db_path)
            runtime = ChannelRuntime(
                state=RuntimeState(
                    channel_id=42,
                    api_id=1,
                    api_hash="hash",
                    phone_number="+77000000000",
                    callback_url="http://app.test/webhook",
                    webhook_secret="secret",
                    runtime_state={
                        "history_sync_requested_at": "history-run-1",
                        "history_sync_state": "running",
                        "history_sync_reader_completed_at": "2026-04-13T00:00:00+00:00",
                    },
                ),
                settings=settings,
                callbacks=callbacks,
                media_store=MediaStore(settings),
                store=store,
            )

            try:
                inserted = await runtime._enqueue_import_event(
                    event=HISTORY_MESSAGE_EVENT,
                    category=HISTORY_OUTBOX_CATEGORY,
                    scope_key="history-run-1",
                    idempotency_key="message:1",
                    payload={"message_id": 1, "chat_id": "7", "peer_user_id": "7"},
                )
                self.assertTrue(inserted)

                await asyncio.sleep(0.1)

                message_deliveries = [
                    delivery
                    for delivery in callbacks.deliveries
                    if delivery["event"] == HISTORY_MESSAGE_EVENT
                ]
                self.assertEqual(1, len(message_deliveries))
                self.assertEqual("completed", runtime.state.runtime_state["history_sync_state"])
                self.assertEqual(1, runtime.state.runtime_state["history_sync_count"])
                self.assertEqual(0, runtime.state.runtime_state["history_sync_pending_count"])
                self.assertEqual(0, runtime.state.runtime_state["history_sync_dead_count"])
            finally:
                await runtime.close()
                store.close()

    async def test_history_event_dead_letters_and_marks_sync_completed_with_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root, max_attempts=1)
            callbacks = _FakeCallbacks(failures_before_success=10)
            store = GatewayStore(settings.state_db_path)
            runtime = ChannelRuntime(
                state=RuntimeState(
                    channel_id=43,
                    api_id=1,
                    api_hash="hash",
                    phone_number="+77000000001",
                    callback_url="http://app.test/webhook",
                    webhook_secret="secret",
                    runtime_state={
                        "history_sync_requested_at": "history-run-2",
                        "history_sync_state": "running",
                        "history_sync_reader_completed_at": "2026-04-13T00:00:00+00:00",
                    },
                ),
                settings=settings,
                callbacks=callbacks,
                media_store=MediaStore(settings),
                store=store,
            )

            try:
                inserted = await runtime._enqueue_import_event(
                    event=HISTORY_MESSAGE_EVENT,
                    category=HISTORY_OUTBOX_CATEGORY,
                    scope_key="history-run-2",
                    idempotency_key="message:2",
                    payload={"message_id": 2, "chat_id": "8", "peer_user_id": "8"},
                )
                self.assertTrue(inserted)

                await asyncio.sleep(0.1)

                self.assertEqual(
                    "completed_with_errors",
                    runtime.state.runtime_state["history_sync_state"],
                )
                self.assertEqual(0, runtime.state.runtime_state["history_sync_count"])
                self.assertEqual(0, runtime.state.runtime_state["history_sync_pending_count"])
                self.assertEqual(1, runtime.state.runtime_state["history_sync_dead_count"])
                self.assertIn("unavailable", runtime.state.runtime_state["history_sync_error"])
            finally:
                await runtime.close()
                store.close()
