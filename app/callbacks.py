import asyncio
import logging
import time
from typing import Any, Dict

import httpx

from app.config import GatewaySettings
from app.security import encode_jwt

logger = logging.getLogger(__name__)


class CallbackClient:
    def __init__(self, settings: GatewaySettings):
        self._settings = settings
        self._client = httpx.AsyncClient(timeout=settings.callback_timeout_seconds)

    async def close(self) -> None:
        await self._client.aclose()

    async def send_event(
        self, *, callback_url: str, webhook_secret: str, channel_id: int, event: str, data: Dict[str, Any]
    ) -> None:
        token = encode_jwt(
            {
                "iss": "telegram-personal-gateway",
                "channel_id": channel_id,
                "event": event,
                "exp": int(time.time()) + 300,
            },
            webhook_secret,
        )
        payload = {"event": event, "data": data}

        last_error: Exception | None = None
        for attempt in range(1, self._settings.callback_max_retries + 1):
            try:
                response = await self._client.post(
                    callback_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {token}"},
                )
                response.raise_for_status()
                return
            except Exception as error:  # pragma: no cover - retried below
                last_error = error
                logger.warning(
                    "callback delivery failed for channel=%s event=%s attempt=%s: %s",
                    channel_id,
                    event,
                    attempt,
                    error,
                )
                if attempt >= self._settings.callback_max_retries:
                    break
                await asyncio.sleep(2 ** (attempt - 1))

        raise RuntimeError(
            f"Failed to deliver callback {event} for channel {channel_id}"
        ) from last_error
