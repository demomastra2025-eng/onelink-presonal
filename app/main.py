import logging
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from telethon import errors

from app.config import load_settings
from app.runtime import RuntimeManager
from app.schemas import (
    ChannelSyncPayload,
    ContactsSyncPayload,
    DeleteMessagesPayload,
    EditMessagePayload,
    HistorySyncPayload,
    MarkReadPayload,
    SendMessagePayload,
    VerifyCodePayload,
    VerifyPasswordPayload,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()
settings = load_settings()
runtime_manager = RuntimeManager(settings)


def verify_internal_token(
    authorization: str = Header(default="", alias="Authorization"),
) -> None:
    expected = f"Bearer {settings.internal_token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        yield
    finally:
        await runtime_manager.shutdown()


app = FastAPI(
    title="Telegram Personal Gateway",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(KeyError)
async def handle_key_error(_: Request, exc: KeyError):
    return JSONResponse(
        status_code=404,
        content={"error": str(exc).strip("'")},
    )


@app.exception_handler(ValueError)
async def handle_value_error(_: Request, exc: ValueError):
    return JSONResponse(
        status_code=422,
        content={"error": str(exc)},
    )


@app.exception_handler(FileNotFoundError)
async def handle_file_not_found(_: Request, exc: FileNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"error": str(exc)},
    )


@app.exception_handler(errors.RPCError)
async def handle_telegram_rpc_error(_: Request, exc: errors.RPCError):
    return JSONResponse(
        status_code=422,
        content={"error": str(exc)},
    )


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "telegram_personal_gateway",
        "public_base_url": settings.public_base_url,
        "synced_channels": len(runtime_manager._runtimes),
    }


@app.post(
    "/internal/channels/{channel_id}/sync",
    dependencies=[Depends(verify_internal_token)],
)
async def sync_channel(channel_id: int, payload: ChannelSyncPayload):
    return await runtime_manager.sync(channel_id, payload)


@app.post(
    "/internal/channels/{channel_id}/auth/request-code",
    dependencies=[Depends(verify_internal_token)],
)
async def request_code(channel_id: int):
    return await runtime_manager.request_login_code(channel_id)


@app.post(
    "/internal/channels/{channel_id}/auth/request-qr",
    dependencies=[Depends(verify_internal_token)],
)
async def request_qr(channel_id: int):
    return await runtime_manager.request_qr_login(channel_id)


@app.post(
    "/internal/channels/{channel_id}/auth/verify-code",
    dependencies=[Depends(verify_internal_token)],
)
async def verify_code(channel_id: int, payload: VerifyCodePayload):
    return await runtime_manager.verify_code(channel_id, payload.code)


@app.post(
    "/internal/channels/{channel_id}/auth/verify-password",
    dependencies=[Depends(verify_internal_token)],
)
async def verify_password(channel_id: int, payload: VerifyPasswordPayload):
    return await runtime_manager.verify_password(channel_id, payload.password)


@app.post(
    "/internal/channels/{channel_id}/reconnect",
    dependencies=[Depends(verify_internal_token)],
)
async def reconnect_channel(channel_id: int):
    return await runtime_manager.reconnect(channel_id)


@app.post(
    "/internal/channels/{channel_id}/history-sync",
    dependencies=[Depends(verify_internal_token)],
)
async def history_sync_channel(
    channel_id: int, payload: HistorySyncPayload | None = None
):
    payload = payload or HistorySyncPayload()
    return await runtime_manager.history_sync(
        channel_id,
        force=payload.force,
        reset_cursor=payload.reset_cursor,
        include_contacts=payload.include_contacts,
    )


@app.post(
    "/internal/channels/{channel_id}/contacts-sync",
    dependencies=[Depends(verify_internal_token)],
)
async def contacts_sync_channel(
    channel_id: int, payload: ContactsSyncPayload | None = None
):
    payload = payload or ContactsSyncPayload()
    return await runtime_manager.contacts_sync(channel_id, force=payload.force)


@app.post(
    "/internal/channels/{channel_id}/disconnect",
    dependencies=[Depends(verify_internal_token)],
)
async def disconnect_channel(channel_id: int):
    return await runtime_manager.disconnect(channel_id)


@app.delete(
    "/internal/channels/{channel_id}",
    dependencies=[Depends(verify_internal_token)],
)
async def teardown_channel(channel_id: int):
    return await runtime_manager.teardown(channel_id)


@app.get(
    "/internal/channels/{channel_id}/diagnostics",
    dependencies=[Depends(verify_internal_token)],
)
async def channel_diagnostics(channel_id: int):
    return await runtime_manager.diagnostics(channel_id)


@app.post(
    "/internal/channels/{channel_id}/messages",
    dependencies=[Depends(verify_internal_token)],
)
async def send_message(channel_id: int, payload: SendMessagePayload):
    return await runtime_manager.send_message(channel_id, payload)


@app.post(
    "/internal/channels/{channel_id}/messages/edit",
    dependencies=[Depends(verify_internal_token)],
)
async def edit_message(channel_id: int, payload: EditMessagePayload):
    return await runtime_manager.edit_message(channel_id, payload)


@app.post(
    "/internal/channels/{channel_id}/messages/delete",
    dependencies=[Depends(verify_internal_token)],
)
async def delete_messages(channel_id: int, payload: DeleteMessagesPayload):
    return await runtime_manager.delete_messages(channel_id, payload)


@app.post(
    "/internal/channels/{channel_id}/mark-read",
    dependencies=[Depends(verify_internal_token)],
)
async def mark_read(channel_id: int, payload: MarkReadPayload):
    return await runtime_manager.mark_read(channel_id, payload)


@app.get(
    "/internal/channels/{channel_id}/contacts/{peer_user_id}/avatar",
    dependencies=[Depends(verify_internal_token)],
)
async def get_profile_avatar(channel_id: int, peer_user_id: str, fingerprint: str):
    stored = await runtime_manager.fetch_profile_avatar(
        channel_id,
        peer_user_id=peer_user_id,
        avatar_fingerprint=fingerprint,
    )
    return FileResponse(
        stored.path,
        media_type=stored.content_type,
        filename=stored.filename,
    )


@app.get("/media/{media_id}")
async def get_media(media_id: str, token: str):
    stored = runtime_manager.media_store.get(media_id, token)
    return FileResponse(
        stored.path,
        media_type=stored.content_type,
        filename=stored.filename,
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app", host=settings.host, port=settings.port, log_level="info"
    )
