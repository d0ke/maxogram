from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiohttp import ClientSession
from maxapi import Bot
from maxapi.client import DefaultConnectionProperties
from maxapi.enums.api_path import ApiPath
from maxapi.enums.http_method import HTTPMethod
from maxapi.enums.message_link_type import MessageLinkType
from maxapi.enums.parse_mode import TextFormat
from maxapi.enums.update import UpdateType
from maxapi.enums.upload_type import UploadType
from maxapi.exceptions.max import MaxApiError
from maxapi.types.input_media import InputMedia
from maxapi.types.message import NewMessageLink

from maxogram.domain import (
    MAX_ALLOWED_UPDATE_TYPES,
    LocalMediaFile,
    MediaKind,
    MediaPresentation,
    PollBatch,
    PollUpdate,
    SendResult,
)
from maxogram.services.dedup import max_update_key
from maxogram.services.media import VIDEO_URL_ORDER

from .base import PlatformDeliveryError, RateLimiter

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DownloadedMediaInfo:
    content_type: str | None = None
    is_gif: bool = False


class MaxClient:
    def __init__(
        self,
        token: str,
        *,
        proxy_url: str | None = None,
        trust_env: bool = False,
    ) -> None:
        kwargs: dict[str, Any] = {"trust_env": trust_env}
        if proxy_url:
            kwargs["proxy"] = proxy_url
        connection = DefaultConnectionProperties(**kwargs)
        self.bot = Bot(token=token, default_connection=connection)
        self.rate_limiter = RateLimiter(max_per_second=30)

    async def poll_updates(
        self,
        cursor: int | None,
        *,
        limit: int,
        poll_timeout: int,
    ) -> PollBatch:
        await self.rate_limiter.wait()
        try:
            response = await self.bot.get_updates(
                limit=limit,
                timeout=poll_timeout,
                marker=cursor,
                types=[UpdateType(item) for item in MAX_ALLOWED_UPDATE_TYPES],
            )
        except MaxApiError as exc:
            raise PlatformDeliveryError(
                str(exc),
                retryable=exc.code >= 500 or exc.code == 429,
                code=str(exc.code),
                http_status=exc.code,
            ) from exc

        updates = response.get("updates") or []
        poll_updates = [
            PollUpdate(max_update_key(update), update)
            for update in updates
            if isinstance(update, dict)
        ]
        marker = response.get("marker")
        return PollBatch(poll_updates, int(marker) if marker is not None else cursor)

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
        await self.rate_limiter.wait()
        rendered_text, text_format = _max_text_payload(text_plain, text_html)
        link = (
            NewMessageLink(type=MessageLinkType.REPLY, mid=reply_to_message_id)
            if reply_to_message_id
            else None
        )
        attachments = (
            [InputMedia(str(media.path), type=_upload_type(media.kind))]
            if media is not None
            else None
        )
        try:
            result = await self.bot.send_message(
                chat_id=int(chat_id),
                text=rendered_text,
                attachments=attachments,
                link=link,
                format=text_format,
                parse_mode=None,
                sleep_after_input_media=False,
            )
        except MaxApiError as exc:
            raise _max_error(exc) from exc
        if result is None:
            raise PlatformDeliveryError(
                "MAX returned empty send result", retryable=True
            )
        raw = result.model_dump(mode="json", by_alias=True, exclude_none=True)
        return SendResult(message_id=str(result.message.body.mid), raw=raw)

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
        _ = chat_id, has_media
        await self.rate_limiter.wait()
        rendered_text, text_format = _max_text_payload(text_plain, text_html)
        try:
            if replacement_media is None:
                payload: dict[str, object] = {"text": rendered_text}
                if text_format is not None:
                    payload["format"] = text_format.value
                await self.bot.request(
                    method=HTTPMethod.PUT,
                    path=ApiPath.MESSAGES,
                    is_return_raw=True,
                    params={"message_id": message_id},
                    json=payload,
                )
            else:
                attachments = [
                    InputMedia(
                        str(replacement_media.path),
                        type=_upload_type(replacement_media.kind),
                    )
                ]
                await self.bot.edit_message(
                    message_id=message_id,
                    text=rendered_text,
                    attachments=attachments,
                    format=text_format,
                    parse_mode=None,
                    sleep_after_input_media=False,
                )
        except MaxApiError as exc:
            raise _max_error(exc) from exc

    async def delete_message(self, chat_id: str, message_id: str) -> None:
        _ = chat_id
        await self.rate_limiter.wait()
        try:
            await self.bot.delete_message(message_id=message_id)
        except MaxApiError as exc:
            raise _max_error(exc) from exc

    async def download_media(
        self,
        media: dict[str, object],
        destination_dir: Path,
    ) -> LocalMediaFile | None:
        source = media.get("source")
        if not isinstance(source, dict):
            return None
        url = _optional_str(source.get("url"))
        token = _optional_str(source.get("token"))
        kind = _media_kind(media.get("kind"))
        if url is None and kind == MediaKind.VIDEO and token is not None:
            url = await self._resolve_video_url(token)
        if url is None:
            return None
        filename = Path(
            _optional_str(media.get("filename")) or _filename_from_url(url)
        ).name
        destination = destination_dir / f"{uuid.uuid4()}-{filename}"
        download_info = await self._download_to_path(url, destination)
        mime_type = _optional_str(media.get("mime_type")) or download_info.content_type
        presentation = _media_presentation(media.get("presentation"))
        resolved_filename = filename
        if kind == MediaKind.IMAGE and presentation is None and _is_gif_media_file(
            filename=filename,
            mime_type=mime_type,
            download_info=download_info,
        ):
            presentation = MediaPresentation.ANIMATION
            mime_type = "image/gif"
            resolved_filename = _animation_filename(filename)
            logger.info(
                "Upgraded MAX image attachment to GIF animation url=%s filename=%s",
                url,
                resolved_filename,
            )
        return LocalMediaFile(
            kind=kind,
            path=destination,
            filename=resolved_filename,
            mime_type=mime_type,
            sticker_variant=_optional_str(media.get("sticker_variant")),
            presentation=presentation,
        )

    async def is_admin(self, chat_id: str, user_id: str) -> bool:
        await self.rate_limiter.wait()
        try:
            result = await self.bot.get_list_admin_chat(chat_id=int(chat_id))
        except MaxApiError:
            return False
        raw = result.model_dump(mode="json", by_alias=True, exclude_none=True)
        admins = raw.get("members") or raw.get("admins") or []
        return any(str(item.get("user_id")) == user_id for item in admins)

    async def close(self) -> None:
        session = getattr(self.bot, "session", None)
        if session is not None and hasattr(session, "close"):
            close_result = session.close()
            if hasattr(close_result, "__await__"):
                await close_result

    async def _resolve_video_url(self, token: str) -> str | None:
        await self.rate_limiter.wait()
        try:
            video = await self.bot.get_video(token)
        except MaxApiError as exc:
            raise _max_error(exc) from exc
        urls = (
            video.urls.model_dump(mode="json", by_alias=True, exclude_none=True)
            if video.urls
            else {}
        )
        for key in VIDEO_URL_ORDER:
            resolved = _optional_str(urls.get(key))
            if resolved:
                return resolved
        return None

    async def _download_to_path(
        self, url: str, destination: Path
    ) -> DownloadedMediaInfo:
        async with ClientSession(
            timeout=self.bot.default_connection.timeout,
            headers=self.bot.headers,
            **self.bot.default_connection.kwargs,
        ) as session:
            response = await session.get(url)
            if not response.ok:
                error_body = await response.text()
                message = (
                    "MAX media download failed with status "
                    f"{response.status}: {error_body}"
                )
                raise PlatformDeliveryError(
                    message,
                    retryable=response.status >= 500 or response.status == 429,
                    code=str(response.status),
                    http_status=response.status,
                )
            content_type = _normalize_content_type(response.headers.get("Content-Type"))
            file_bytes = await response.read()
            await asyncio.to_thread(destination.write_bytes, file_bytes)
        return DownloadedMediaInfo(
            content_type=content_type,
            is_gif=_is_gif_content_type(content_type) or _looks_like_gif(file_bytes),
        )


def _max_error(exc: MaxApiError) -> PlatformDeliveryError:
    raw = exc.raw.lower() if isinstance(exc.raw, str) else str(exc.raw).lower()
    permanent = exc.code in {400, 401, 403, 404} and "attachment.not.ready" not in raw
    return PlatformDeliveryError(
        str(exc),
        retryable=not permanent,
        code=str(exc.code),
        http_status=exc.code,
    )


def _upload_type(kind: MediaKind) -> UploadType:
    if kind == MediaKind.IMAGE:
        return UploadType.IMAGE
    if kind == MediaKind.VIDEO:
        return UploadType.VIDEO
    if kind in {MediaKind.AUDIO, MediaKind.VOICE}:
        return UploadType.AUDIO
    return UploadType.FILE


def _max_text_payload(
    text_plain: str,
    text_html: str | None,
) -> tuple[str, TextFormat | None]:
    if len(text_plain) > 3999:
        return text_plain[:3999], None
    if text_html is not None and len(text_html) <= 3999:
        return text_html, TextFormat.HTML
    return text_plain, None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _media_kind(value: object) -> MediaKind:
    return MediaKind(str(value))


def _media_presentation(value: object) -> MediaPresentation | None:
    if value is None:
        return None
    return MediaPresentation(str(value))


def _filename_from_url(url: str) -> str:
    candidate = Path(url.split("?", 1)[0]).name
    return candidate or "max-media.bin"


def _normalize_content_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.split(";", 1)[0].strip().casefold()
    return normalized or None


def _is_gif_content_type(value: str | None) -> bool:
    return value == "image/gif"


def _looks_like_gif(file_bytes: bytes) -> bool:
    return file_bytes.startswith((b"GIF87a", b"GIF89a"))


def _is_gif_media_file(
    *,
    filename: str,
    mime_type: str | None,
    download_info: DownloadedMediaInfo,
) -> bool:
    if download_info.is_gif:
        return True
    if _is_gif_content_type(mime_type):
        return True
    return Path(filename).suffix.casefold() == ".gif"


def _animation_filename(filename: str) -> str:
    path = Path(filename)
    suffix = path.suffix.casefold()
    if suffix == ".gif":
        return path.name
    if suffix in {"", ".bin"}:
        return f"{path.name}.gif"
    return path.name
