import tempfile
import unittest
from pathlib import Path

from app.config import GatewaySettings
from app.media_store import MediaStore
from app.outbox import GatewayStore
from app.runtime import ChannelRuntime, RuntimeState


class _FakeCallbacks:
    async def send_event(self, **_kwargs):
        return None

    async def close(self):
        return None


class _FakeAuthorizedClient:
    def is_connected(self):
        return True

    async def connect(self):
        return None

    async def is_user_authorized(self):
        return True


class GatewayContactsFirstTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_manual_full_history_with_contacts_waits_for_contacts_completion(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root)
            store = GatewayStore(settings.state_db_path)
            runtime = ChannelRuntime(
                state=RuntimeState(
                    channel_id=77,
                    api_id=1,
                    api_hash="hash",
                    phone_number="+77000000000",
                    callback_url="http://app.test/webhook",
                    webhook_secret="secret",
                    runtime_state={},
                ),
                settings=settings,
                callbacks=_FakeCallbacks(),
                media_store=MediaStore(settings),
                store=store,
            )
            runtime._client = _FakeAuthorizedClient()
            runtime._history_sync_task = None
            runtime._contacts_sync_task = None

            history_requests = []
            contacts_requests = []

            def fake_schedule_history_sync(*, reason: str, force: bool = False, reset_cursor: bool = False):
                history_requests.append(
                    {
                        "reason": reason,
                        "force": force,
                        "reset_cursor": reset_cursor,
                    }
                )

            def fake_schedule_contacts_sync(*, reason: str, force: bool = False):
                contacts_requests.append(
                    {
                        "reason": reason,
                        "force": force,
                    }
                )

            runtime._schedule_history_sync = fake_schedule_history_sync
            runtime._schedule_contacts_sync = fake_schedule_contacts_sync

            try:
                await runtime.history_sync(force=True, reset_cursor=True, include_contacts=True)

                self.assertEqual(
                    [{"reason": "manual", "force": True}],
                    contacts_requests,
                )
                self.assertEqual([], history_requests)
                self.assertEqual(
                    "manual",
                    runtime.state.runtime_state["history_sync_after_contacts_reason"],
                )

                runtime.state.runtime_state["contacts_sync_state"] = "completed"
                queued_history_started = runtime._maybe_schedule_history_after_contacts()

                self.assertTrue(queued_history_started)
                self.assertEqual(
                    [{"reason": "manual", "force": True, "reset_cursor": True}],
                    history_requests,
                )
            finally:
                await runtime.close()
                store.close()
