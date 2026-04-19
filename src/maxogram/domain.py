from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class Platform(StrEnum):
    TELEGRAM = "telegram"
    MAX = "max"


class BridgeStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    DELETED = "deleted"


class RowStatus(StrEnum):
    NEW = "new"
    PROCESSED = "processed"
    IGNORED = "ignored"


class TaskStatus(StrEnum):
    READY = "ready"
    INFLIGHT = "inflight"
    RETRY_WAIT = "retry_wait"
    DONE = "done"
    DEAD = "dead"


class EventType(StrEnum):
    MESSAGE_CREATED = "message.created"
    MESSAGE_EDITED = "message.edited"
    MESSAGE_DELETED = "message.deleted"
    SERVICE = "service"


class OutboxAction(StrEnum):
    SEND = "send"
    EDIT = "edit"
    DELETE = "delete"


class MediaKind(StrEnum):
    IMAGE = "image"
    VIDEO = "video"
    DOCUMENT = "document"
    AUDIO = "audio"
    VOICE = "voice"


class MediaPresentation(StrEnum):
    ANIMATION = "animation"


class MediaGroupKind(StrEnum):
    PHOTO_VIDEO_CHUNK = "photo_video_chunk"


class MutationType(StrEnum):
    EDIT = "edit"
    DELETE = "delete"


class DeliveryOutcome(StrEnum):
    SUCCESS = "success"
    RETRY = "retry"
    DEAD = "dead"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ChatRef:
    platform: Platform
    chat_id: str


@dataclass(frozen=True, slots=True)
class UserRef:
    platform: Platform
    user_id: str


@dataclass(frozen=True, slots=True)
class UserIdentity:
    platform: Platform
    user_id: str
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    is_bot: bool | None = None


@dataclass(frozen=True, slots=True)
class PollUpdate:
    update_key: str
    raw: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PollBatch:
    updates: list[PollUpdate]
    next_cursor: int | None


@dataclass(frozen=True, slots=True)
class SendResult:
    message_id: str
    raw: dict[str, Any]
    member_message_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class LocalMediaFile:
    kind: MediaKind
    path: Path
    filename: str
    mime_type: str | None = None
    sticker_variant: str | None = None
    presentation: MediaPresentation | None = None
    cleanup_after_use: bool = True


@dataclass(frozen=True, slots=True)
class CommandContext:
    platform: Platform
    chat_id: str
    user_id: str
    message_id: str | None
    text: str
    reply_to_user_id: str | None = None
    reply_to_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class CommandReply:
    platform: Platform
    chat_id: str
    text: str


TELEGRAM_ALLOWED_UPDATES = [
    "message",
    "edited_message",
    "chat_member",
    "my_chat_member",
]

MAX_ALLOWED_UPDATE_TYPES = [
    "message_created",
    "message_edited",
    "message_removed",
    "bot_added",
    "bot_removed",
    "chat_title_changed",
    "message_chat_created",
    "user_added",
    "user_removed",
]
