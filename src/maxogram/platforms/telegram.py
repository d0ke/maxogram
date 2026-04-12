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
from maxogram.services.media import TELEGRAM_DOWNLOAD_LIMIT_BYTES

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
        media: LocalMediaFile | None = None,
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
            raise PlatformDeliveryError(
                str(exc),
                retryable=_is_retryable_telegram(exc),
                code=exc.__class__.__name__,
            ) from exc
        raw: dict[str, Any] = message.model_dump(
            mode="json", by_alias=True, exclude_none=True
        )
        return SendResult(message_id=str(message.message_id), raw=raw)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        has_media: bool = False,
        replacement_media: LocalMediaFile | None = None,
    ) -> None:
        try:
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
            raise PlatformDeliveryError(
                str(exc),
                retryable=_is_retryable_telegram(exc),
                code=exc.__class__.__name__,
            ) from exc

    async def delete_message(self, chat_id: str, message_id: str) -> None:
        try:
            await self.bot.delete_message(
                chat_id=_telegram_chat_id(chat_id), message_id=int(message_id)
            )
        except TelegramAPIError as exc:
            raise PlatformDeliveryError(
                str(exc),
                retryable=_is_retryable_telegram(exc),
                code=exc.__class__.__name__,
            ) from exc

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
            raise PlatformDeliveryError(
                str(exc),
                retryable=_is_retryable_telegram(exc),
                code=exc.__class__.__name__,
            ) from exc
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
            raise PlatformDeliveryError(
                str(exc),
                retryable=_is_retryable_telegram(exc),
                code=exc.__class__.__name__,
            ) from exc
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
    return not isinstance(exc, TelegramForbiddenError)
