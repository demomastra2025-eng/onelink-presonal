import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from app.config import GatewaySettings
from app.media_store import MediaStore


class MediaStoreTest(unittest.TestCase):
    def build_settings(
        self,
        root: Path,
        *,
        media_ttl_seconds: int = 900,
        avatar_ttl_seconds: int = 2_592_000,
    ) -> GatewaySettings:
        return GatewaySettings(
            internal_token="token",
            public_base_url="http://127.0.0.1:8000",
            media_secret="media-secret",
            media_ttl_seconds=media_ttl_seconds,
            avatar_ttl_seconds=avatar_ttl_seconds,
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

    def token_from_signed_url(self, signed_url: str) -> str:
        return parse_qs(urlparse(signed_url).query)["token"][0]

    def test_media_survives_media_store_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root)
            source = root / "voice.ogg"
            source.write_bytes(b"voice-bytes")

            first_store = MediaStore(settings)
            stored = first_store.store_path(
                source_path=source,
                filename="voice.ogg",
                content_type="audio/ogg",
            )
            token = self.token_from_signed_url(first_store.signed_url(stored.media_id))

            restarted_store = MediaStore(settings)
            reloaded = restarted_store.get(stored.media_id, token)

            self.assertEqual("voice.ogg", reloaded.filename)
            self.assertEqual("audio/ogg", reloaded.content_type)
            self.assertTrue(reloaded.path.exists())
            self.assertEqual(b"voice-bytes", reloaded.path.read_bytes())

    def test_cleanup_expired_removes_disk_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root, media_ttl_seconds=1)
            source = root / "expired.ogg"
            source.write_bytes(b"expired-bytes")

            store = MediaStore(settings)
            stored = store.store_path(
                source_path=source,
                filename="expired.ogg",
                content_type="audio/ogg",
            )

            time.sleep(2)
            store.cleanup_expired()

            self.assertFalse(stored.path.exists())
            self.assertFalse((settings.media_dir / f"{stored.media_id}.json").exists())

    def test_single_item_list_metadata_is_loaded_for_backward_compatibility(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root)
            media_dir = settings.media_dir
            media_dir.mkdir(parents=True, exist_ok=True)
            media_id = "legacy-media"
            media_path = media_dir / "legacy-media-voice.ogg"
            media_path.write_bytes(b"legacy-bytes")
            (media_dir / f"{media_id}.json").write_text(
                '[{"media_id":"legacy-media","path":"legacy-media-voice.ogg","filename":"voice.ogg","content_type":"audio/ogg","expires_at":4102444800}]',
                encoding="utf-8",
            )

            store = MediaStore(settings)
            token = self.token_from_signed_url(store.signed_url(media_id))
            reloaded = store.get(media_id, token)

            self.assertEqual("voice.ogg", reloaded.filename)
            self.assertEqual("audio/ogg", reloaded.content_type)
            self.assertEqual(b"legacy-bytes", reloaded.path.read_bytes())

    def test_invalid_metadata_shape_is_cleaned_up(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root)
            media_dir = settings.media_dir
            media_dir.mkdir(parents=True, exist_ok=True)
            media_id = "broken-media"
            (media_dir / f"{media_id}.json").write_text(
                '["bad-shape"]', encoding="utf-8"
            )

            store = MediaStore(settings)

            self.assertIsNone(store._load_from_disk(media_id))
            self.assertFalse((media_dir / f"{media_id}.json").exists())

    def test_profile_avatar_survives_media_store_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root)
            source = root / "avatar.jpg"
            source.write_bytes(b"avatar-bytes")

            first_store = MediaStore(settings)
            stored = first_store.store_profile_avatar_path(
                channel_id=42,
                peer_user_id="23",
                avatar_fingerprint="telegram-photo-23",
                source_path=source,
                filename="avatar.jpg",
                content_type="image/jpeg",
            )

            restarted_store = MediaStore(settings)
            reloaded = restarted_store.get_profile_avatar(
                channel_id=42,
                peer_user_id="23",
                avatar_fingerprint="telegram-photo-23",
            )

            self.assertEqual(stored.filename, reloaded.filename)
            self.assertEqual("image/jpeg", reloaded.content_type)
            self.assertTrue(reloaded.path.exists())
            self.assertEqual(b"avatar-bytes", reloaded.path.read_bytes())

    def test_cleanup_expired_profile_avatars_removes_disk_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = self.build_settings(root, avatar_ttl_seconds=1)
            source = root / "avatar.jpg"
            source.write_bytes(b"avatar-bytes")

            store = MediaStore(settings)
            stored = store.store_profile_avatar_path(
                channel_id=42,
                peer_user_id="23",
                avatar_fingerprint="telegram-photo-23",
                source_path=source,
                filename="avatar.jpg",
                content_type="image/jpeg",
            )

            time.sleep(2)
            store.cleanup_expired_profile_avatars()

            self.assertFalse(stored.path.exists())
            self.assertFalse(
                store.has_profile_avatar(
                    channel_id=42,
                    peer_user_id="23",
                    avatar_fingerprint="telegram-photo-23",
                )
            )
