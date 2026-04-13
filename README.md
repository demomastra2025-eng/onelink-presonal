# Telegram Personal Gateway

`telegram-personal-gateway` is a dedicated MTProto runtime for OneLink.

It runs as a separate Python/FastAPI service backed by Telethon and is responsible for:

- maintaining Telegram user sessions
- SMS code and 2FA login flow
- incoming private-chat updates
- outbound send/edit/delete/read operations
- signed callbacks back into OneLink
- signed temporary media URLs for inbound files
- durable SQLite outbox for history and contacts import callbacks

This service is not a generic Chatwoot bridge anymore. It is a OneLink-specific runtime for `Telegram Personal`.

## Runtime contract

OneLink calls the internal gateway API to:

- sync channel configuration
- request login code
- verify login code
- verify 2FA password
- reconnect or disconnect a runtime
- trigger bounded history sync
- send, edit, delete, and mark messages read
- fetch diagnostics

The gateway sends signed callbacks back into OneLink for:

- inbound messages
- edited messages
- deleted messages
- read updates
- activity events
- runtime state updates

## Required environment variables

```env
TELEGRAM_PERSONAL_GATEWAY_TOKEN=replace-with-strong-shared-token
TELEGRAM_PERSONAL_GATEWAY_PUBLIC_BASE_URL=https://telegram-gateway.example.com
```

## Optional environment variables

```env
TELEGRAM_PERSONAL_GATEWAY_MEDIA_SECRET=
TELEGRAM_PERSONAL_GATEWAY_MEDIA_DIR=/data/telegram-personal-gateway
TELEGRAM_PERSONAL_GATEWAY_HOST=0.0.0.0
TELEGRAM_PERSONAL_GATEWAY_PORT=8000
TELEGRAM_PERSONAL_GATEWAY_CALLBACK_TIMEOUT_SECONDS=15
TELEGRAM_PERSONAL_GATEWAY_CALLBACK_MAX_RETRIES=3
TELEGRAM_PERSONAL_GATEWAY_STATE_DB_PATH=/data/telegram-personal-gateway/state.sqlite3
TELEGRAM_PERSONAL_GATEWAY_OUTBOX_BATCH_SIZE=50
TELEGRAM_PERSONAL_GATEWAY_OUTBOX_POLL_INTERVAL_SECONDS=1
TELEGRAM_PERSONAL_GATEWAY_OUTBOX_MAX_DELIVERY_ATTEMPTS=20
TELEGRAM_PERSONAL_GATEWAY_OUTBOX_RETENTION_HOURS=168
TELEGRAM_PERSONAL_GATEWAY_MEDIA_TTL_SECONDS=900
TELEGRAM_PERSONAL_GATEWAY_HISTORY_DIALOG_LIMIT=20
TELEGRAM_PERSONAL_GATEWAY_HISTORY_MESSAGE_LIMIT=50
TELEGRAM_PERSONAL_GATEWAY_HISTORY_LOOKBACK_HOURS=24
TELEGRAM_PERSONAL_GATEWAY_HISTORY_OVERLAP_SECONDS=300
```

If `TELEGRAM_PERSONAL_GATEWAY_MEDIA_SECRET` is omitted, the service reuses `TELEGRAM_PERSONAL_GATEWAY_TOKEN`.

History and contacts sync now enqueue import callbacks into a local SQLite outbox first. This lets long sync runs continue even if OneLink is temporarily unavailable, and delivery resumes automatically from the gateway.

## Local run

```bash
cp .env_template .env
poetry install
poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Docker run

```bash
docker build -t telegram-personal-gateway .

docker run --rm \
  -p 8000:8000 \
  --env-file .env \
  -v telegram_personal_gateway_data:/data/telegram-personal-gateway \
  telegram-personal-gateway
```

## OneLink integration variables

The OneLink Rails app must also know how to reach this service:

```env
TELEGRAM_PERSONAL_GATEWAY_URL=http://telegram-personal-gateway:8000
TELEGRAM_PERSONAL_GATEWAY_TOKEN=replace-with-the-same-shared-token
```

## Health check

```bash
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{
  "ok": true,
  "service": "telegram_personal_gateway"
}
```
