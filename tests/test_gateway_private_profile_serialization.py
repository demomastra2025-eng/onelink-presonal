import tempfile
import unittest
from datetime import datetime, timezone
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


class _FakeClient:
    def __init__(self, *, peer_user=None):
        self.peer_user = peer_user
        self.get_entity_calls = []
        self.disconnect_calls = 0

    def is_connected(self):
        return False

    async def disconnect(self):
        self.disconnect_calls += 1
        return None

    async def get_entity(self, raw):
        self.get_entity_calls.append(raw)
        return self.peer_user


class _FakeMessage:
    def __init__(self, *, message_id, peer_user_id, sender, chat, text, out):
        self.id = message_id
        self.peer_id = types.PeerUser(peer_user_id)
        self.out = out
        self.message = text
        self.media = None
        self.reply_to = None
        self.date = datetime(2026, 4, 13, 14, 18, 38, tzinfo=timezone.utc)
        self._sender = sender
        self._chat = chat

    async def get_sender(self):
        return self._sender

    async def get_chat(self):
        return self._chat


class _FakeAlbumEvent:
    def __init__(self, *, messages, grouped_id, text):
        self.messages = messages
        self.grouped_id = grouped_id
        self.text = text


class GatewayPrivateProfileSerializationTest(unittest.IsolatedAsyncioTestCase):
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

        async def _profile_photo_url(user):
            return f"https://avatar.test/{getattr(user, 'id', 'unknown')}"

        async def _extract_media(_message):
            return [], None, None

        runtime._ensure_client = _noop
        runtime._connect_if_needed = _noop
        runtime._emit_runtime_update = _noop
        runtime._profile_photo_url = _profile_photo_url
        runtime._extract_media = _extract_media
        return runtime

    async def test_serialize_message_event_uses_private_dialog_peer_profile(self):
        sender = types.User(
            id=9001,
            is_self=True,
            first_name="One link",
            last_name="Менеджер",
            username="Bakhitov",
            phone="77066318623",
            photo=types.UserProfilePhoto(photo_id=9001001, dc_id=2),
        )
        peer_user = types.User(
            id=134527512,
            contact=True,
            first_name="Мой",
            last_name="Теле2",
            username="tele2",
            phone="77475318623",
            photo=types.UserProfilePhoto(photo_id=134527512001, dc_id=2),
        )
        runtime = await self.build_runtime(_FakeClient(peer_user=peer_user))
        self.addAsyncCleanup(runtime.close)

        message = _FakeMessage(
            message_id=16442,
            peer_user_id=134527512,
            sender=sender,
            chat=peer_user,
            text="салам",
            out=True,
        )

        payload = await runtime._serialize_message_event(message)

        self.assertEqual("134527512", payload["peer_user_id"])
        self.assertEqual("9001", payload["sender_id"])
        self.assertEqual("Мой", payload["first_name"])
        self.assertEqual("Теле2", payload["last_name"])
        self.assertEqual("tele2", payload["username"])
        self.assertEqual("77475318623", payload["phone_number"])
        self.assertEqual("https://avatar.test/134527512", payload["avatar_url"])
        self.assertEqual("134527512001", payload["avatar_fingerprint"])

    async def test_serialize_album_event_uses_private_dialog_peer_profile(self):
        sender = types.User(
            id=9001,
            is_self=True,
            first_name="One link",
            last_name="Менеджер",
            username="Bakhitov",
            phone="77066318623",
            photo=types.UserProfilePhoto(photo_id=9001001, dc_id=2),
        )
        peer_user = types.User(
            id=134527512,
            contact=True,
            first_name="Мой",
            last_name="Теле2",
            username="tele2",
            phone="77475318623",
            photo=types.UserProfilePhoto(photo_id=134527512001, dc_id=2),
        )
        runtime = await self.build_runtime(_FakeClient(peer_user=peer_user))
        self.addAsyncCleanup(runtime.close)

        message = _FakeMessage(
            message_id=16443,
            peer_user_id=134527512,
            sender=sender,
            chat=peer_user,
            text="альбом",
            out=True,
        )
        event = _FakeAlbumEvent(messages=[message], grouped_id=777, text="альбом")

        payload = await runtime._serialize_album_event(event)

        self.assertEqual("134527512", payload["peer_user_id"])
        self.assertEqual("9001", payload["sender_id"])
        self.assertEqual("Мой", payload["first_name"])
        self.assertEqual("Теле2", payload["last_name"])
        self.assertEqual("tele2", payload["username"])
        self.assertEqual("77475318623", payload["phone_number"])
        self.assertEqual("https://avatar.test/134527512", payload["avatar_url"])
        self.assertEqual("134527512001", payload["avatar_fingerprint"])
