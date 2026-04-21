import tempfile
import unittest
from pathlib import Path

from telethon.tl import types

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


class _FakeProfilePhotoClient(_FakeAuthorizedClient):
    def __init__(self, *, peer_user=None):
        super().__init__()
        self.peer_user = peer_user
        self.get_entity_calls = []
        self.download_profile_photo_calls = []

    async def get_entity(self, raw):
        self.get_entity_calls.append(raw)
        return self.peer_user

    async def download_profile_photo(self, sender, file, download_big=False):
        self.download_profile_photo_calls.append(
            {
                "sender_id": getattr(sender, "id", None),
                "download_big": download_big,
            }
        )
        target = Path(f"{file}.jpg")
        target.write_bytes(
            f"avatar-{getattr(getattr(sender, 'photo', None), 'photo_id', 'none')}".encode(
                "utf-8"
            )
        )
        return str(target)


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

    async def build_runtime(self, fake_client):
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
        runtime._client = fake_client
        self.addAsyncCleanup(runtime.close)
        return runtime

    async def test_connect_marks_authorized_session_as_connected(self):
        runtime = await self.build_runtime(_FakeAuthorizedClient())

        await runtime._connect_if_needed()

        self.assertEqual("connected", runtime.state.connection_state)
        self.assertEqual("connected", runtime.state.lifecycle_state)

    async def test_profile_photo_cache_uses_avatar_fingerprint(self):
        client = _FakeProfilePhotoClient()
        runtime = await self.build_runtime(client)

        first_sender = types.User(
            id=23,
            first_name="Sojan",
            photo=types.UserProfilePhoto(photo_id=1001, dc_id=2),
        )
        second_sender = types.User(
            id=23,
            first_name="Sojan",
            photo=types.UserProfilePhoto(photo_id=1002, dc_id=2),
        )

        first_url = await runtime._profile_photo_url(first_sender)
        second_url = await runtime._profile_photo_url(second_sender)

        self.assertNotEqual(first_url, second_url)
        self.assertEqual(2, len(client.download_profile_photo_calls))
        self.assertTrue(
            runtime._media_store.has_profile_avatar(
                channel_id=42,
                peer_user_id="23",
                avatar_fingerprint="1001",
            )
        )
        self.assertTrue(
            runtime._media_store.has_profile_avatar(
                channel_id=42,
                peer_user_id="23",
                avatar_fingerprint="1002",
            )
        )

    async def test_fetch_profile_avatar_recovers_from_avatar_cache_miss(self):
        peer_user = types.User(
            id=23,
            first_name="Sojan",
            photo=types.UserProfilePhoto(photo_id=1002, dc_id=2),
        )
        client = _FakeProfilePhotoClient(peer_user=peer_user)
        runtime = await self.build_runtime(client)

        stored = await runtime.fetch_profile_avatar(
            peer_user_id="23",
            avatar_fingerprint="1002",
        )

        self.assertTrue(stored.path.exists())
        self.assertEqual(1, client.connect_calls)
        self.assertEqual([23], client.get_entity_calls)
        self.assertEqual(1, len(client.download_profile_photo_calls))

        cached = await runtime.fetch_profile_avatar(
            peer_user_id="23",
            avatar_fingerprint="1002",
        )
        self.assertEqual(stored.path, cached.path)
        self.assertEqual(1, len(client.download_profile_photo_calls))
