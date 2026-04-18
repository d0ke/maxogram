from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from maxogram.domain import EventType, MediaGroupKind, Platform, UserIdentity
from maxogram.services.dedup import canonical_dedup_key, stable_json_hash
from maxogram.services.media import build_media_plan
from maxogram.services.rendering import sanitize_alias
from maxogram.services.text_formatting import (
    max_markup_to_html,
    telegram_entities_to_html,
)


@dataclass(frozen=True, slots=True)
class NormalizedUpdate:
    platform: Platform
    event_type: EventType
    dedup_key: str
    chat_id: str
    user_id: str | None
    message_id: str | None
    event_version: str | int | None
    text: str | None
    formatted_html: str | None
    happened_at: datetime
    identity: UserIdentity | None
    reply_to_message_id: str | None = None
    reply_to_user_id: str | None = None
    forwarded: bool = False
    media_hint: str | None = None
    payload: dict[str, Any] | None = None
    is_command: bool = False
    is_bot_message: bool = False


@dataclass(frozen=True, slots=True)
class _UnwrappedBridgeText:
    text: str | None
    prefix_utf16: int


def normalize_update(
    platform: Platform,
    raw: dict[str, Any],
) -> NormalizedUpdate | None:
    if platform == Platform.TELEGRAM:
        return _normalize_telegram(raw)
    return _normalize_max(raw)


def normalize_telegram_media_group(
    *,
    group_key: str,
    members: list[dict[str, Any]],
    has_flushed: bool,
) -> NormalizedUpdate | None:
    if not members:
        return None
    first_message = members[0]
    chat = _dict_value(first_message.get("chat"))
    sender = _telegram_actor(first_message)
    chat_id = str(chat.get("id"))
    user_id = str(sender.get("id")) if sender.get("id") is not None else None
    reply = first_message.get("reply_to_message")
    reply_to_message_id = None
    reply_to_user_id = None
    if isinstance(reply, dict):
        reply_message_id = reply.get("message_id")
        reply_to_message_id = (
            str(reply_message_id) if reply_message_id is not None else None
        )
        reply_sender = _telegram_actor(reply)
        if reply_sender.get("id") is not None:
            reply_to_user_id = str(reply_sender["id"])

    caption_text: str | None = None
    caption_entities: list[dict[str, Any]] | None = None
    media_items: list[dict[str, Any]] = []
    source_member_message_ids: list[str] = []
    happened_at_values: list[int] = []
    forwarded = False
    for member in members:
        media = build_media_plan(Platform.TELEGRAM, member)
        if (
            not media.supported
            or media.kind not in {"image", "video"}
            or not isinstance(media.payload, dict)
        ):
            return None
        media_items.append(media.payload)
        message_id = member.get("message_id")
        if message_id is not None:
            source_member_message_ids.append(str(message_id))
        text, entities = _telegram_message_content(member)
        if caption_text is None and text is not None:
            caption_text = text
            caption_entities = entities
        forwarded = forwarded or any(key.startswith("forward_") for key in member)
        happened_at_values.append(
            _int_value(member.get("edit_date") or member.get("date") or 0)
        )

    happened_at = _telegram_group_happened_at(happened_at_values)
    formatted_html = telegram_entities_to_html(caption_text, caption_entities)
    identity = (
        UserIdentity(
            platform=Platform.TELEGRAM,
            user_id=user_id,
            username=sender.get("username"),
            first_name=sender.get("first_name"),
            last_name=sender.get("last_name"),
            is_bot=sender.get("is_bot"),
        )
        if user_id
        else None
    )
    event_type = EventType.MESSAGE_EDITED if has_flushed else EventType.MESSAGE_CREATED
    event_version = (
        stable_json_hash({"group_key": group_key, "members": members})
        if event_type == EventType.MESSAGE_EDITED
        else None
    )
    media_group_payload = {
        "supported": True,
        "group_kind": MediaGroupKind.PHOTO_VIDEO_CHUNK.value,
        "group_key": group_key,
        "text_hint": _photo_video_chunk_text_hint(media_items),
        "items": media_items,
        "source_member_message_ids": source_member_message_ids,
    }
    return NormalizedUpdate(
        platform=Platform.TELEGRAM,
        event_type=event_type,
        dedup_key=canonical_dedup_key(
            Platform.TELEGRAM,
            chat_id,
            group_key,
            event_type,
            event_version,
        ),
        chat_id=chat_id,
        user_id=user_id,
        message_id=group_key,
        event_version=event_version,
        text=caption_text,
        formatted_html=formatted_html,
        happened_at=happened_at,
        identity=identity,
        reply_to_message_id=reply_to_message_id,
        reply_to_user_id=reply_to_user_id,
        forwarded=forwarded,
        media_hint=media_group_payload["text_hint"],
        payload={
            "raw_messages": members,
            "media_group": media_group_payload,
        },
        is_command=False,
        is_bot_message=bool(sender.get("is_bot")),
    )


def _normalize_telegram(raw: dict[str, Any]) -> NormalizedUpdate | None:
    event_type = EventType.MESSAGE_CREATED
    message = raw.get("message")
    event_version: int | str | None = None
    if message is None and raw.get("edited_message") is not None:
        message = raw["edited_message"]
        event_type = EventType.MESSAGE_EDITED
        event_version = message.get("edit_date") or raw.get("update_id")
    if not isinstance(message, dict):
        return None

    chat = _dict_value(message.get("chat"))
    sender = _telegram_actor(message)
    chat_id = str(chat.get("id"))
    message_id = str(message.get("message_id"))
    user_id = str(sender.get("id")) if sender.get("id") is not None else None
    text, entities = _telegram_message_content(message)
    forwarded = any(key.startswith("forward_") for key in message)
    unwrapped_bridge_text = False
    if forwarded and _telegram_forward_source_is_bot(message):
        unwrapped = _unwrap_bridge_text(text)
        if unwrapped is not None:
            text = unwrapped.text
            entities = _trim_spans(
                entities,
                start_key="offset",
                length_key="length",
                removed_utf16=unwrapped.prefix_utf16,
                text=text,
            )
            unwrapped_bridge_text = True
    date_value = (
        message.get("edit_date")
        if event_type == EventType.MESSAGE_EDITED
        and message.get("edit_date") is not None
        else message.get("date") or raw.get("update_id") or 0
    )
    happened_at = datetime.fromtimestamp(_int_value(date_value), tz=UTC)
    media = build_media_plan(Platform.TELEGRAM, message)
    formatted_html = telegram_entities_to_html(text, entities)
    identity = (
        UserIdentity(
            platform=Platform.TELEGRAM,
            user_id=user_id,
            username=sender.get("username"),
            first_name=sender.get("first_name"),
            last_name=sender.get("last_name"),
            is_bot=sender.get("is_bot"),
        )
        if user_id
        else None
    )
    reply = message.get("reply_to_message")
    reply_to_message_id = None
    reply_to_user_id = None
    if isinstance(reply, dict):
        reply_message_id = reply.get("message_id")
        reply_to_message_id = (
            str(reply_message_id) if reply_message_id is not None else None
        )
        reply_sender = _telegram_actor(reply)
        if reply_sender.get("id") is not None:
            reply_to_user_id = str(reply_sender["id"])

    return NormalizedUpdate(
        platform=Platform.TELEGRAM,
        event_type=event_type,
        dedup_key=canonical_dedup_key(
            Platform.TELEGRAM, chat_id, message_id, event_type, event_version
        ),
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        event_version=event_version,
        text=text,
        formatted_html=formatted_html,
        happened_at=happened_at,
        identity=identity,
        reply_to_message_id=reply_to_message_id,
        reply_to_user_id=reply_to_user_id,
        forwarded=forwarded,
        media_hint=media.text_hint,
        payload={"raw_message": message, "media": asdict(media)},
        is_command=bool(
            isinstance(text, str)
            and text.startswith("/")
            and not unwrapped_bridge_text
        ),
        is_bot_message=bool(sender.get("is_bot")),
    )


def _normalize_max(raw: dict[str, Any]) -> NormalizedUpdate | None:
    update_type = raw.get("update_type") or raw.get("type")
    event_version: int | str | None = None
    if update_type == "message_removed":
        chat_id = raw.get("chat_id")
        message_id = raw.get("message_id")
        if chat_id is None or message_id is None:
            return None
        user_id = raw.get("user_id")
        event_version = raw.get("timestamp") or message_id
        happened_at = _max_datetime(raw.get("timestamp"))
        identity = (
            UserIdentity(platform=Platform.MAX, user_id=str(user_id))
            if user_id is not None
            else None
        )
        return NormalizedUpdate(
            platform=Platform.MAX,
            event_type=EventType.MESSAGE_DELETED,
            dedup_key=canonical_dedup_key(
                Platform.MAX,
                str(chat_id),
                str(message_id),
                EventType.MESSAGE_DELETED,
                event_version,
            ),
            chat_id=str(chat_id),
            user_id=str(user_id) if user_id is not None else None,
            message_id=str(message_id),
            event_version=event_version,
            text=None,
            formatted_html=None,
            happened_at=happened_at,
            identity=identity,
            payload={"raw_message": raw},
            is_command=False,
            is_bot_message=False,
        )

    message = raw.get("message") if isinstance(raw.get("message"), dict) else None
    if message is None:
        return None
    body = _dict_value(message.get("body"))
    recipient = _dict_value(message.get("recipient"))
    sender = _dict_value(message.get("sender"))
    chat_id = str(recipient.get("chat_id") or recipient.get("user_id"))
    message_id = str(body.get("mid"))
    user_id = str(sender.get("user_id")) if sender.get("user_id") is not None else None
    link = message.get("link") if isinstance(message.get("link"), dict) else {}
    linked_message = _dict_value(link.get("message"))
    content_message = (
        linked_message
        if link.get("type") == "forward"
        and _max_body_is_empty(body)
        and linked_message
        else message
    )
    content_body = (
        content_message
        if content_message is linked_message
        else _dict_value(content_message.get("body"))
    )

    event_type = EventType.MESSAGE_CREATED
    if update_type == "message_edited":
        event_type = EventType.MESSAGE_EDITED
        event_version = stable_json_hash(raw)

    timestamp = (
        raw.get("timestamp")
        or message.get("timestamp")
        or body.get("timestamp")
        or body.get("seq")
        or 0
    )
    happened_at = _max_datetime(timestamp)
    media_group = _max_photo_video_chunk_payload(
        content_message,
        chat_id=chat_id,
        message_id=message_id,
    )
    media = build_media_plan(Platform.MAX, content_message)
    text = _string_value(content_body.get("text"))
    markup_value = content_body.get("markup")
    markup = markup_value if isinstance(markup_value, list) else None
    unwrapped_bridge_text = False
    if link.get("type") == "forward" and _max_forward_source_is_bot(link):
        unwrapped = _unwrap_bridge_text(text)
        if unwrapped is not None:
            text = unwrapped.text
            markup = _trim_spans(
                markup,
                start_key="from",
                length_key="length",
                removed_utf16=unwrapped.prefix_utf16,
                text=text,
            )
            unwrapped_bridge_text = True
    formatted_html = max_markup_to_html(text, markup)
    identity = (
        UserIdentity(
            platform=Platform.MAX,
            user_id=user_id,
            username=sender.get("username"),
            first_name=sender.get("first_name") or sender.get("name"),
            last_name=sender.get("last_name"),
            is_bot=sender.get("is_bot"),
        )
        if user_id
        else None
    )
    reply_mid = linked_message.get("mid") if link.get("type") == "reply" else None
    return NormalizedUpdate(
        platform=Platform.MAX,
        event_type=event_type,
        dedup_key=canonical_dedup_key(
            Platform.MAX, chat_id, message_id, event_type, event_version
        ),
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        event_version=event_version,
        text=text,
        formatted_html=formatted_html,
        happened_at=happened_at,
        identity=identity,
        reply_to_message_id=str(reply_mid) if reply_mid is not None else None,
        forwarded=link.get("type") == "forward",
        media_hint=(
            str(media_group.get("text_hint"))
            if isinstance(media_group, dict)
            else media.text_hint
        ),
        payload=(
            {"raw_message": content_message, "media_group": media_group}
            if isinstance(media_group, dict)
            else {"raw_message": content_message, "media": asdict(media)}
        ),
        is_command=bool(
            isinstance(text, str)
            and text.startswith("/")
            and not unwrapped_bridge_text
        ),
        is_bot_message=bool(sender.get("is_bot")),
    )


def _max_datetime(value: object) -> datetime:
    try:
        number = int(value)  # type: ignore[arg-type, call-overload]
    except (TypeError, ValueError):
        return datetime.now(UTC)
    if number > 10_000_000_000:
        number = number // 1000
    if number <= 0:
        return datetime.now(UTC)
    return datetime.fromtimestamp(number, tz=UTC)


def _telegram_group_happened_at(values: list[int]) -> datetime:
    timestamp = max(values) if values else 0
    if timestamp <= 0:
        return datetime.now(UTC)
    return datetime.fromtimestamp(timestamp, tz=UTC)


def _dict_value(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_value(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _telegram_actor(message: dict[str, Any]) -> dict[str, Any]:
    actor = message.get("from_user")
    if isinstance(actor, dict):
        return actor
    actor = message.get("from")
    if isinstance(actor, dict):
        return actor
    return {}


def _telegram_message_content(
    message: dict[str, Any],
) -> tuple[str | None, list[dict[str, Any]] | None]:
    text = _string_value(message.get("text"))
    if text is not None:
        entities = message.get("entities")
        return text, entities if isinstance(entities, list) else None
    caption = _string_value(message.get("caption"))
    if caption is not None:
        entities = message.get("caption_entities")
        return caption, entities if isinstance(entities, list) else None
    return None, None


def _telegram_forward_source_is_bot(message: dict[str, Any]) -> bool:
    if bool(_dict_value(message.get("forward_from")).get("is_bot")):
        return True
    origin = _dict_value(message.get("forward_origin"))
    if bool(_dict_value(origin.get("sender_user")).get("is_bot")):
        return True
    return any(
        _telegram_chat_looks_botish(candidate)
        for candidate in (
            message.get("forward_from_chat"),
            origin.get("sender_chat"),
            origin.get("chat"),
        )
    )


def _telegram_chat_looks_botish(value: object) -> bool:
    chat = _dict_value(value)
    username = _string_value(chat.get("username"))
    return bool(username and username.lower().endswith("bot"))


def _max_body_is_empty(body: dict[str, Any]) -> bool:
    if _string_value(body.get("text")):
        return False
    attachments = body.get("attachments")
    if isinstance(attachments, list) and attachments:
        return False
    markup = body.get("markup")
    return not (isinstance(markup, list) and markup)


def _max_forward_source_is_bot(link: dict[str, Any]) -> bool:
    sender = _dict_value(link.get("sender"))
    if bool(sender.get("is_bot")):
        return True
    username = _string_value(sender.get("username"))
    return bool(username and username.lower().endswith("bot"))


def _max_photo_video_chunk_payload(
    message: dict[str, Any],
    *,
    chat_id: str,
    message_id: str,
) -> dict[str, Any] | None:
    body = message.get("body") if isinstance(message.get("body"), dict) else message
    attachments = body.get("attachments") if isinstance(body, dict) else None
    if not isinstance(attachments, list) or len(attachments) < 2:
        return None

    media_items: list[dict[str, Any]] = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            return None
        media = build_media_plan(
            Platform.MAX,
            {"body": {"attachments": [attachment]}},
        )
        if (
            not media.supported
            or media.kind not in {"image", "video"}
            or not isinstance(media.payload, dict)
        ):
            return None
        media_items.append(media.payload)

    if len(media_items) < 2:
        return None
    return {
        "supported": True,
        "group_kind": MediaGroupKind.PHOTO_VIDEO_CHUNK.value,
        "group_key": f"{Platform.MAX.value}:{chat_id}:{message_id}",
        "text_hint": _photo_video_chunk_text_hint(media_items),
        "items": media_items,
        "source_member_message_ids": [message_id],
    }


def _photo_video_chunk_text_hint(media_items: list[dict[str, Any]]) -> str:
    kinds = {str(item.get("kind")) for item in media_items}
    if kinds == {"image"}:
        return "[photo group]"
    if kinds == {"video"}:
        return "[video group]"
    return "[photo/video group]"


def _unwrap_bridge_text(text: str | None) -> _UnwrappedBridgeText | None:
    if text is None:
        return None
    lines = text.split("\n")
    line_index = 0
    prefix_chars = 0
    while line_index < len(lines) and _is_bridge_prefix_line(lines[line_index]):
        prefix_chars += len(lines[line_index]) + 1
        line_index += 1
    if line_index >= len(lines):
        return None
    first_line = lines[line_index]
    alias, separator, suffix = first_line.partition(":")
    if not separator:
        return None
    try:
        if sanitize_alias(alias) != alias:
            return None
    except ValueError:
        return None
    body_start_chars = prefix_chars + len(alias) + len(separator)
    if suffix.startswith(" "):
        body_start_chars += 1
        first_body = suffix[1:]
    else:
        first_body = suffix
    body_lines = [first_body, *lines[line_index + 1 :]]
    body = "\n".join(body_lines)
    return _UnwrappedBridgeText(
        text=body if body != "" else None,
        prefix_utf16=_utf16_length(text[:body_start_chars]),
    )


def _is_bridge_prefix_line(line: str) -> bool:
    return line == "[forwarded]" or (
        line.startswith("[reply to ") and line.endswith("]")
    )


def _trim_spans(
    items: list[dict[str, Any]] | None,
    *,
    start_key: str,
    length_key: str,
    removed_utf16: int,
    text: str | None,
) -> list[dict[str, Any]] | None:
    if not items or removed_utf16 <= 0 or text is None:
        return items
    body_utf16 = _utf16_length(text)
    trimmed: list[dict[str, Any]] = []
    for item in items:
        start = _int_value(item.get(start_key))
        length = _int_value(item.get(length_key))
        if start < 0 or length <= 0:
            continue
        end = start + length
        if end <= removed_utf16:
            continue
        new_start = max(0, start - removed_utf16)
        new_end = min(body_utf16, end - removed_utf16)
        if new_end <= new_start:
            continue
        trimmed_item = dict(item)
        trimmed_item[start_key] = new_start
        trimmed_item[length_key] = new_end - new_start
        trimmed.append(trimmed_item)
    return trimmed or None


def _utf16_length(text: str) -> int:
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


def _int_value(value: object) -> int:
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0
