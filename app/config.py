import os
from pathlib import Path

from pydantic import BaseModel, Field


class GatewaySettings(BaseModel):
    internal_token: str
    public_base_url: str = "http://127.0.0.1:8000"
    media_secret: str
    media_ttl_seconds: int = 900
    avatar_ttl_seconds: int = 2_592_000
    state_db_path: Path
    callback_timeout_seconds: int = 15
    callback_max_retries: int = 3
    outbox_batch_size: int = 50
    outbox_poll_interval_seconds: float = 1.0
    outbox_max_delivery_attempts: int = 20
    outbox_retention_hours: int = 168
    host: str = "0.0.0.0"
    port: int = 8000
    media_dir: Path = Field(
        default_factory=lambda: Path(
            os.getenv("TELEGRAM_PERSONAL_MEDIA_DIR", "/tmp/telegram-personal-gateway")
        )
    )
    device_model: str = "OneLink Telegram Personal Gateway"
    system_version: str = "1.0"
    app_version: str = "1.0"
    lang_code: str = "en"
    system_lang_code: str = "en-US"
    history_dialog_limit: int = 20
    history_message_limit: int = 50
    history_lookback_hours: int = 24
    history_overlap_seconds: int = 300
    contacts_dialog_limit: int = 0
    contacts_include_saved: bool = True
    contacts_include_dialogs: bool = True


def load_settings() -> GatewaySettings:
    media_dir = Path(
        os.getenv("TELEGRAM_PERSONAL_MEDIA_DIR", "/tmp/telegram-personal-gateway")
    )
    return GatewaySettings(
        internal_token=os.environ["TELEGRAM_PERSONAL_GATEWAY_TOKEN"],
        public_base_url=os.getenv(
            "TELEGRAM_PERSONAL_GATEWAY_PUBLIC_BASE_URL", "http://127.0.0.1:8000"
        ).rstrip("/"),
        media_secret=os.getenv(
            "TELEGRAM_PERSONAL_GATEWAY_MEDIA_SECRET",
            os.environ["TELEGRAM_PERSONAL_GATEWAY_TOKEN"],
        ),
        media_ttl_seconds=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_MEDIA_TTL_SECONDS", "900")
        ),
        avatar_ttl_seconds=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_AVATAR_TTL_SECONDS", "2592000")
        ),
        callback_timeout_seconds=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_CALLBACK_TIMEOUT_SECONDS", "15")
        ),
        callback_max_retries=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_CALLBACK_MAX_RETRIES", "3")
        ),
        state_db_path=Path(
            os.getenv(
                "TELEGRAM_PERSONAL_GATEWAY_STATE_DB_PATH",
                str(media_dir / "state.sqlite3"),
            )
        ),
        outbox_batch_size=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_OUTBOX_BATCH_SIZE", "50")
        ),
        outbox_poll_interval_seconds=float(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_OUTBOX_POLL_INTERVAL_SECONDS", "1")
        ),
        outbox_max_delivery_attempts=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_OUTBOX_MAX_DELIVERY_ATTEMPTS", "20")
        ),
        outbox_retention_hours=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_OUTBOX_RETENTION_HOURS", "168")
        ),
        host=os.getenv("TELEGRAM_PERSONAL_GATEWAY_HOST", "0.0.0.0"),
        port=int(os.getenv("TELEGRAM_PERSONAL_GATEWAY_PORT", "8000")),
        media_dir=media_dir,
        history_dialog_limit=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_HISTORY_DIALOG_LIMIT", "20")
        ),
        history_message_limit=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_HISTORY_MESSAGE_LIMIT", "50")
        ),
        history_lookback_hours=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_HISTORY_LOOKBACK_HOURS", "24")
        ),
        history_overlap_seconds=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_HISTORY_OVERLAP_SECONDS", "300")
        ),
        contacts_dialog_limit=int(
            os.getenv("TELEGRAM_PERSONAL_GATEWAY_CONTACTS_DIALOG_LIMIT", "0")
        ),
        contacts_include_saved=os.getenv(
            "TELEGRAM_PERSONAL_GATEWAY_CONTACTS_INCLUDE_SAVED", "true"
        ).lower()
        in {"1", "true", "yes", "on"},
        contacts_include_dialogs=os.getenv(
            "TELEGRAM_PERSONAL_GATEWAY_CONTACTS_INCLUDE_DIALOGS", "true"
        ).lower()
        in {"1", "true", "yes", "on"},
    )
