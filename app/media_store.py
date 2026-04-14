import json
import mimetypes
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from app.config import GatewaySettings
from app.security import decode_jwt, encode_jwt


@dataclass
class StoredMedia:
    media_id: str
    path: Path
    filename: str
    content_type: str
    expires_at: int


class MediaStore:
    def __init__(self, settings: GatewaySettings):
        self._settings = settings
        self._media: Dict[str, StoredMedia] = {}
        self._settings.media_dir.mkdir(parents=True, exist_ok=True)

    def _metadata_path(self, media_id: str) -> Path:
        return self._settings.media_dir / f"{media_id}.json"

    def _write_metadata(self, stored: StoredMedia) -> None:
        self._metadata_path(stored.media_id).write_text(
            json.dumps(
                {
                    "media_id": stored.media_id,
                    "path": stored.path.name,
                    "filename": stored.filename,
                    "content_type": stored.content_type,
                    "expires_at": stored.expires_at,
                }
            ),
            encoding="utf-8",
        )

    def _load_from_disk(self, media_id: str) -> StoredMedia | None:
        metadata_path = self._metadata_path(media_id)
        if not metadata_path.exists():
            return None
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            metadata_path.unlink(missing_ok=True)
            return None

        payload = self._coerce_metadata_payload(payload)
        if payload is None:
            metadata_path.unlink(missing_ok=True)
            return None

        relative_path = payload.get("path")
        if not relative_path:
            metadata_path.unlink(missing_ok=True)
            return None
        stored_path = self._settings.media_dir / relative_path
        if not stored_path.exists():
            metadata_path.unlink(missing_ok=True)
            return None
        stored = StoredMedia(
            media_id=media_id,
            path=stored_path,
            filename=payload.get("filename") or stored_path.name,
            content_type=payload.get("content_type") or "application/octet-stream",
            expires_at=int(payload.get("expires_at") or 0),
        )
        self._media[media_id] = stored
        return stored

    def _delete_media(self, media_id: str, stored: StoredMedia | None = None) -> None:
        stored = stored or self._media.get(media_id)
        if stored and stored.path.exists():
            stored.path.unlink(missing_ok=True)
        self._media.pop(media_id, None)
        self._metadata_path(media_id).unlink(missing_ok=True)

    def store_path(
        self,
        *,
        source_path: str | Path,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> StoredMedia:
        self.cleanup_expired()
        source = Path(source_path)
        media_id = uuid.uuid4().hex
        target_name = filename or source.name or f"{media_id}.bin"
        target = self._settings.media_dir / f"{media_id}-{target_name}"
        shutil.copyfile(source, target)
        resolved_content_type = (
            content_type
            or mimetypes.guess_type(target_name)[0]
            or "application/octet-stream"
        )
        stored = StoredMedia(
            media_id=media_id,
            path=target,
            filename=target_name,
            content_type=resolved_content_type,
            expires_at=int(time.time()) + self._settings.media_ttl_seconds,
        )
        self._media[media_id] = stored
        self._write_metadata(stored)
        return stored

    def signed_url(self, media_id: str) -> str:
        token = encode_jwt(
            {
                "media_id": media_id,
                "exp": int(time.time()) + self._settings.media_ttl_seconds,
            },
            self._settings.media_secret,
        )
        return f"{self._settings.public_base_url}/media/{media_id}?token={token}"

    def get(self, media_id: str, token: str) -> StoredMedia:
        payload = decode_jwt(token, self._settings.media_secret)
        if payload.get("media_id") != media_id:
            raise ValueError("Invalid media token")
        stored = self._media.get(media_id) or self._load_from_disk(media_id)
        if stored is None or stored.expires_at < int(time.time()):
            self._delete_media(media_id, stored)
            raise FileNotFoundError("Media not found or expired")
        return stored

    def cleanup_expired(self) -> None:
        now = int(time.time())
        expired_ids = [
            media_id
            for media_id, stored in self._media.items()
            if stored.expires_at < now or not stored.path.exists()
        ]
        for media_id in expired_ids:
            self._delete_media(media_id)
        for metadata_path in self._settings.media_dir.glob("*.json"):
            media_id = metadata_path.stem
            if media_id in self._media:
                continue
            stored = self._load_from_disk(media_id)
            if stored is None or stored.expires_at < now or not stored.path.exists():
                self._delete_media(media_id, stored)

    def _coerce_metadata_payload(self, payload: object) -> dict | None:
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
            return payload[0]
        return None
