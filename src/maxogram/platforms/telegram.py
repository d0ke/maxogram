from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Any

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.types import (
    FSInputFile,
    InputMediaAnimation,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    ReplyParameters,
)
from aiogram.utils.serialization import deserialize_telegram_object_to_python

from maxogram.domain import (
    TELEGRAM_ALLOWED_UPDATES,
    LocalMediaFile,
    MediaKind,
    MediaPresentation,
    PollBatch,
    PollUpdate,
    SendResult,
)
from maxogram.metrics import telegram_skipped_update_total
from maxogram.services.media import (
    OUTBOUND_MEDIA_COUNT_LIMIT,
    TELEGRAM_DOWNLOAD_LIMIT_BYTES,
)

from .base import PlatformDeliveryError

TelegramEditableInputMedia = (
    InputMediaAnimation
    | InputMediaAudio
    | InputMediaDocument
    | InputMediaPhoto
    | InputMediaVideo
)

logger = logging.getLogger(__name__)


class TelegramClient:
    def __init__(self, token: str, *, proxy_url: str | None = None) -> None:
        session = AiohttpSession(proxy=proxy_url) if proxy_url else None
        self.bot = Bot(token=token, session=session)

    async def poll_updates(
        self,
        cursor: int | None,
        *,
        limit: int,
        poll_timeout: int,
    ) -> PollBatch:
        try:
            updates = await self.bot.get_updates(
                offset=cursor,
                limit=limit,
                timeout=poll_timeout,
                allowed_updates=TELEGRAM_ALLOWED_UPDATES,
            )
        except TelegramAPIError as exc:
            raise PlatformDeliveryError(
                str(exc), retryable=True, code=exc.__class__.__name__
            ) from exc
        poll_updates: list[PollUpdate] = []
        next_cursor = cursor
        for update in updates:
            next_cursor = update.update_id + 1
            try:
                raw = deserialize_telegram_object_to_python(update)
            except Exception:
                logger.exception(
                    "Skipping Telegram update_id=%s after serialization failure",
                    update.update_id,
                )
                telegram_skipped_update_total.labels("serialization_error").inc()
                continue
            if not isinstance(raw, dict):
                logger.warning(
                    "Skipping Telegram update_id=%s because serialized payload is %s",
                    update.update_id,
                    type(raw).__name__,
                )
                telegram_skipped_update_total.labels("invalid_payload").inc()
                continue
            poll_updates.append(PollUpdate(str(update.update_id), raw))
        return PollBatch(poll_updates, next_cursor)

    async def send_text(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
    ) -> SendResult:
        return await self.send_message(
            chat_id,
            text_plain,
            text_html=text_html,
            reply_to_message_id=reply_to_message_id,
            media=None,
        )

    async def send_message(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
        media: LocalMediaFile | list[LocalMediaFile] | None = None,
    ) -> SendResult:
        try:
            reply_parameters = _reply_parameters(reply_to_message_id)
            if media is None:
                rendered_text, parse_mode = _telegram_text_payload(
                    text_plain,
                    text_html,
                )
                message = await self.bot.send_message(
                    chat_id=_telegram_chat_id(chat_id),
                    text=rendered_text,
                    parse_mode=parse_mode,
                    reply_parameters=reply_parameters,
                )
            elif isinstance(media, list):
                if len(media) == 1:
                    message = await self._send_media(
                        chat_id=chat_id,
                        text_plain=text_plain,
                        text_html=text_html,
                        reply_parameters=reply_parameters,
                        media=media[0],
                    )
                else:
                    return await self._send_media_group(
                        chat_id=chat_id,
                        text_plain=text_plain,
                        text_html=text_html,
                        reply_parameters=reply_parameters,
                        media_items=media,
                    )
            else:
                message = await self._send_media(
                    chat_id=chat_id,
                    text_plain=text_plain,
                    text_html=text_html,
                    reply_parameters=reply_parameters,
                    media=media,
                )
        except TelegramRetryAfter as exc:
            raise PlatformDeliveryError(
                str(exc), retryable=True, code="rate_limited", http_status=429
            ) from exc
        except TelegramForbiddenError as exc:
            raise PlatformDeliveryError(
                str(exc), retryable=False, code="forbidden", http_status=403
            ) from exc
        except TelegramAPIError as exc:
            raise _telegram_delivery_error(exc) from exc
        message_id = str(message.message_id)
        raw = _serialize_sent_message(
            message,
            chat_id=chat_id,
            message_id=message_id,
        )
        return SendResult(message_id=message_id, raw=raw)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        has_media: bool = False,
        replacement_media: LocalMediaFile | list[LocalMediaFile] | None = None,
    ) -> None:
        try:
            if isinstance(replacement_media, list):
                raise PlatformDeliveryError(
                    "Telegram media-group edits must be delete-and-recreate",
                    retryable=False,
                    code="unsupported_media_group_edit",
                )
            if replacement_media is not None:
                await self.bot.edit_message_media(
                    chat_id=_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    media=_telegram_input_media(
                        replacement_media,
                        text_plain,
                        text_html,
                    ),
                )
            elif has_media:
                caption, parse_mode = _telegram_caption_payload(text_plain, text_html)
                await self.bot.edit_message_caption(
                    chat_id=_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    caption=caption,
                    parse_mode=parse_mode,
                )
            else:
                rendered_text, parse_mode = _telegram_text_payload(
                    text_plain,
                    text_html,
                )
                await self.bot.edit_message_text(
                    chat_id=_telegram_chat_id(chat_id),
                    message_id=int(message_id),
                    text=rendered_text,
                    parse_mode=parse_mode,
                )
        except TelegramAPIError as exc:
            raise _telegram_delivery_error(exc) from exc

    async def delete_message(self, chat_id: str, message_id: str) -> None:
        try:
            await self.bot.delete_message(
                chat_id=_telegram_chat_id(chat_id), message_id=int(message_id)
            )
        except TelegramAPIError as exc:
            if _is_missing_telegram_message_error(exc):
                return
            raise _telegram_delivery_error(exc) from exc

    async def download_media(
        self,
        media: dict[str, object],
        destination_dir: Path,
    ) -> LocalMediaFile | None:
        source = media.get("source")
        if not isinstance(source, dict):
            return None
        file_id = _optional_str(source.get("file_id"))
        if file_id is None:
            return None
        size = _optional_int(media.get("size"))
        if size is not None and size > TELEGRAM_DOWNLOAD_LIMIT_BYTES:
            return None
        try:
            telegram_file = await self.bot.get_file(file_id)
        except TelegramAPIError as exc:
            raise _telegram_delivery_error(exc) from exc
        file_path = telegram_file.file_path
        if not file_path:
            return None
        filename = _resolve_filename(
            media=media,
            fallback=Path(file_path).name or "telegram-media.bin",
        )
        destination = destination_dir / f"{uuid.uuid4()}-{filename}"
        try:
            await self.bot.download_file(file_path, destination=destination)
        except TelegramAPIError as exc:
            raise _telegram_delivery_error(exc) from exc
        return LocalMediaFile(
            kind=_media_kind(media.get("kind")),
            path=destination,
            filename=filename,
            mime_type=_optional_str(media.get("mime_type")),
            sticker_variant=_optional_str(media.get("sticker_variant")),
            presentation=_media_presentation(media.get("presentation")),
        )

    async def is_admin(self, chat_id: str, user_id: str) -> bool:
        try:
            member = await self.bot.get_chat_member(
                chat_id=_telegram_chat_id(chat_id), user_id=int(user_id)
            )
        except TelegramAPIError:
            return False
        return getattr(member, "status", None) in {"creator", "administrator"}

    async def close(self) -> None:
        await self.bot.session.close()

    async def _send_media(
        self,
        *,
        chat_id: str,
        text_plain: str,
        text_html: str | None,
        reply_parameters: ReplyParameters | None,
        media: LocalMediaFile,
    ) -> Any:
        input_file = FSInputFile(media.path, filename=media.filename)
        caption, parse_mode = _telegram_caption_payload(text_plain, text_html)
        chat_ref = _telegram_chat_id(chat_id)
        if media.presentation == MediaPresentation.ANIMATION:
            return await self.bot.send_animation(
                chat_id=chat_ref,
                animation=input_file,
                caption=caption,
                parse_mode=parse_mode,
                reply_parameters=reply_parameters,
            )
        if media.kind == MediaKind.IMAGE:
            return await self.bot.send_photo(
                chat_id=chat_ref,
                photo=input_file,
                caption=caption,
                parse_mode=parse_mode,
                reply_parameters=reply_parameters,
            )
        if media.kind == MediaKind.VIDEO:
            return await self.bot.send_video(
                chat_id=chat_ref,
                video=input_file,
                caption=caption,
                parse_mode=parse_mode,
                reply_parameters=reply_parameters,
            )
        if media.kind == MediaKind.DOCUMENT:
            return await self.bot.send_document(
                chat_id=chat_ref,
                document=input_file,
                caption=caption,
                parse_mode=parse_mode,
                reply_parameters=reply_parameters,
            )
        if media.kind == MediaKind.AUDIO:
            return await self.bot.send_audio(
                chat_id=chat_ref,
                audio=input_file,
                caption=caption,
                parse_mode=parse_mode,
                reply_parameters=reply_parameters,
            )
        if media.kind == MediaKind.VOICE:
            return await self.bot.send_voice(
                chat_id=chat_ref,
                voice=input_file,
                caption=caption,
                parse_mode=parse_mode,
                reply_parameters=reply_parameters,
            )
        raise PlatformDeliveryError(
            f"Unsupported Telegram media kind: {media.kind}",
            retryable=False,
            code="unsupported_media_kind",
        )

    async def _send_media_group(
        self,
        *,
        chat_id: str,
        text_plain: str,
        text_html: str | None,
        reply_parameters: ReplyParameters | None,
        media_items: list[LocalMediaFile],
    ) -> SendResult:
        if len(media_items) < 2:
            raise PlatformDeliveryError(
                "Telegram media group must contain at least two items",
                retryable=False,
                code="invalid_media_group",
            )
        if len(media_items) > OUTBOUND_MEDIA_COUNT_LIMIT:
            raise PlatformDeliveryError(
                "Telegram media group exceeds the 10-item limit",
                retryable=False,
                code="invalid_media_group",
            )
        caption, parse_mode = _telegram_caption_payload(text_plain, text_html)
        chat_ref = _telegram_chat_id(chat_id)
        input_media = _telegram_input_media_group_chunk(
            media_items,
            caption=caption,
            parse_mode=parse_mode,
        )
        sent_messages = await self.bot.send_media_group(
            chat_id=chat_ref,
            media=input_media,
            reply_parameters=reply_parameters,
        )
        if not sent_messages:
            raise PlatformDeliveryError(
                "Telegram returned empty media-group send result",
                retryable=True,
            )
        message_ids = tuple(str(message.message_id) for message in sent_messages)
        raw = {"message_ids": [int(message_id) for message_id in message_ids]}
        return SendResult(
            message_id=message_ids[0],
            raw=raw,
            member_message_ids=message_ids,
        )


def _telegram_chat_id(value: str) -> int | str:
    try:
        return int(value)
    except ValueError:
        return value


def _reply_parameters(reply_to_message_id: str | None) -> ReplyParameters | None:
    if reply_to_message_id is None:
        return None
    return ReplyParameters(message_id=int(reply_to_message_id))


def _telegram_text_payload(
    text_plain: str,
    text_html: str | None,
) -> tuple[str, str | None]:
    if len(text_plain) > 4096:
        return text_plain[:4096], None
    if text_html is not None and len(text_html) <= 4096:
        return text_html, "HTML"
    return text_plain, None


def _telegram_caption_payload(
    text_plain: str,
    text_html: str | None,
) -> tuple[str | None, str | None]:
    if not text_plain:
        return None, None
    if len(text_plain) > 1024:
        return text_plain[:1024], None
    if text_html is not None and len(text_html) <= 1024:
        return text_html, "HTML"
    return text_plain, None


def _telegram_input_media(
    media: LocalMediaFile,
    text_plain: str,
    text_html: str | None,
) -> TelegramEditableInputMedia:
    input_file = FSInputFile(media.path, filename=media.filename)
    caption, parse_mode = _telegram_caption_payload(text_plain, text_html)
    if media.presentation == MediaPresentation.ANIMATION:
        return InputMediaAnimation(
            media=input_file,
            caption=caption,
            parse_mode=parse_mode,
        )
    if media.kind == MediaKind.IMAGE:
        return InputMediaPhoto(
            media=input_file,
            caption=caption,
            parse_mode=parse_mode,
        )
    if media.kind == MediaKind.VIDEO:
        return InputMediaVideo(
            media=input_file,
            caption=caption,
            parse_mode=parse_mode,
        )
    if media.kind == MediaKind.DOCUMENT:
        return InputMediaDocument(
            media=input_file,
            caption=caption,
            parse_mode=parse_mode,
        )
    if media.kind == MediaKind.AUDIO:
        return InputMediaAudio(
            media=input_file,
            caption=caption,
            parse_mode=parse_mode,
        )
    if media.kind == MediaKind.VOICE:
        raise PlatformDeliveryError(
            "Replacing Telegram voice media during edit is unsupported",
            retryable=False,
            code="unsupported_voice_media_edit",
        )
    raise PlatformDeliveryError(
        f"Unsupported Telegram media kind for edit: {media.kind}",
        retryable=False,
        code="unsupported_media_kind",
    )


def _telegram_input_media_group_chunk(
    media_items: list[LocalMediaFile],
    *,
    caption: str | None,
    parse_mode: str | None,
) -> list[InputMediaPhoto | InputMediaVideo]:
    input_media: list[InputMediaPhoto | InputMediaVideo] = []
    for index, media in enumerate(media_items):
        input_file = FSInputFile(media.path, filename=media.filename)
        item_caption = caption if index == 0 else None
        item_parse_mode = parse_mode if index == 0 else None
        if media.kind == MediaKind.IMAGE:
            input_media.append(
                InputMediaPhoto(
                    media=input_file,
                    caption=item_caption,
                    parse_mode=item_parse_mode,
                )
            )
            continue
        if media.kind == MediaKind.VIDEO:
            input_media.append(
                InputMediaVideo(
                    media=input_file,
                    caption=item_caption,
                    parse_mode=item_parse_mode,
                )
            )
            continue
        raise PlatformDeliveryError(
            f"Unsupported Telegram media-group kind: {media.kind}",
            retryable=False,
            code="unsupported_media_group_kind",
        )
    return input_media


def _resolve_filename(media: dict[str, object], fallback: str) -> str:
    filename = _optional_str(media.get("filename"))
    return Path(filename or fallback).name


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _optional_int(value: object) -> int | None:
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value)
        return None
    except (TypeError, ValueError):
        return None


def _media_kind(value: object) -> MediaKind:
    return MediaKind(str(value))


def _media_presentation(value: object) -> MediaPresentation | None:
    if value is None:
        return None
    return MediaPresentation(str(value))


def _is_retryable_telegram(exc: TelegramAPIError) -> bool:
    return not (
        isinstance(exc, TelegramForbiddenError) or _is_telegram_entity_too_large(exc)
    )


def _telegram_delivery_error(exc: TelegramAPIError) -> PlatformDeliveryError:
    if _is_telegram_entity_too_large(exc):
        return PlatformDeliveryError(
            str(exc),
            retryable=False,
            code="entity_too_large",
            http_status=413,
        )
    return PlatformDeliveryError(
        str(exc),
        retryable=_is_retryable_telegram(exc),
        code=exc.__class__.__name__,
    )


def _is_telegram_entity_too_large(exc: TelegramAPIError) -> bool:
    if exc.__class__.__name__ == "TelegramEntityTooLarge":
        return True
    message = str(exc).casefold()
    return "request entity too large" in message or "entity too large" in message


def _is_missing_telegram_message_error(exc: TelegramAPIError) -> bool:
    message = str(exc).casefold()
    return "message to delete not found" in message


def _serialize_sent_message(
    message: Any,
    *,
    chat_id: str,
    message_id: str,
) -> dict[str, Any]:
    try:
        raw = deserialize_telegram_object_to_python(message)
    except Exception as exc:
        logger.warning(
            "Falling back to minimal Telegram send result serialization "
            "chat_id=%s message_id=%s error=%s: %s",
            chat_id,
            message_id,
            exc.__class__.__name__,
            exc,
        )
        return _minimal_message_raw(message_id)
    if isinstance(raw, dict):
        return raw
    logger.warning(
        "Falling back to minimal Telegram send result serialization "
        "chat_id=%s message_id=%s payload_type=%s",
        chat_id,
        message_id,
        type(raw).__name__,
    )
    return _minimal_message_raw(message_id)


def _minimal_message_raw(message_id: str) -> dict[str, Any]:
    return {"message_id": int(message_id)}
