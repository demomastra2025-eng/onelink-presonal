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
    def __init__(self):
        self.connect_calls = 0
        self.disconnect_calls = 0

    def is_connected(self):
        return False

    async def connect(self):
        self.connect_calls += 1
        return None

    async def is_user_authorized(self):
        return True

    async def disconnect(self):
        self.disconnect_calls += 1
        return None


class GatewayRuntimeStateTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_connect_marks_authorized_session_as_connected(self):
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
                string_session="authorized-session",
                lifecycle_state="pending_auth",
            ),
            settings=settings,
            callbacks=_FakeCallbacks(),
            media_store=MediaStore(settings),
            store=store,
        )
        runtime._client = _FakeAuthorizedClient()
        self.addAsyncCleanup(runtime.close)

        await runtime._connect_if_needed()

        self.assertEqual("connected", runtime.state.connection_state)
        self.assertEqual("connected", runtime.state.lifecycle_state)
