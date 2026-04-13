from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class ChannelSyncPayload(BaseModel):
    api_id: int
    api_hash: str
    phone_number: str
    string_session: Optional[str] = None
    callback_url: HttpUrl
    webhook_secret: str
    runtime_state: Dict[str, Any] = Field(default_factory=dict)


class VerifyCodePayload(BaseModel):
    code: str


class VerifyPasswordPayload(BaseModel):
    password: str


class HistorySyncPayload(BaseModel):
    force: bool = True
    reset_cursor: bool = False
    include_contacts: bool = False


class ContactsSyncPayload(BaseModel):
    force: bool = True


class OutboundAttachmentPayload(BaseModel):
    file_type: str
    url: HttpUrl
    filename: Optional[str] = None
    content_type: Optional[str] = None
    voice_note: bool = False


class SendMessagePayload(BaseModel):
    recipient_id: str
    chat_id: Optional[str] = None
    text: Optional[str] = None
    reply_to_message_id: Optional[str] = None
    attachments: List[OutboundAttachmentPayload] = Field(default_factory=list)


class MarkReadPayload(BaseModel):
    recipient_id: str
    chat_id: Optional[str] = None
    max_id: str


class EditMessagePayload(BaseModel):
    recipient_id: str
    chat_id: Optional[str] = None
    message_id: str
    text: str


class DeleteMessagesPayload(BaseModel):
    recipient_id: str
    chat_id: Optional[str] = None
    message_ids: List[str] = Field(default_factory=list)
