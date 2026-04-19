from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from maxogram.domain import LocalMediaFile, MediaKind, MediaPresentation, Platform
from maxogram.metrics import telegram_media_oversize_total
from maxogram.services.dedup import stable_json_hash

TELEGRAM_DOWNLOAD_LIMIT_BYTES = 20 * 1024 * 1024
TELEGRAM_UPLOAD_PHOTO_LIMIT_BYTES = 10 * 1024 * 1024
TELEGRAM_UPLOAD_FILE_LIMIT_BYTES = 50 * 1024 * 1024
MAX_UPLOAD_LIMIT_BYTES = 50 * 1024 * 1024
OUTBOUND_MEDIA_COUNT_LIMIT = 10
OUTBOUND_MEDIA_PIECE_BUDGET_BYTES = 48 * 1024 * 1024
VIDEO_URL_ORDER = (
    "mp4_1080",
    "mp4_720",
    "mp4_480",
    "mp4_360",
    "mp4_240",
    "mp4_144",
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MediaPlan:
    supported: bool
    kind: str | None = None
    text_hint: str | None = None
    payload: dict[str, Any] | None = None


def build_media_plan(platform: Platform, raw_message: dict[str, Any]) -> MediaPlan:
    if platform == Platform.TELEGRAM:
        return _telegram_media_plan(raw_message)
    return _max_media_plan(raw_message)


def _telegram_media_plan(message: dict[str, Any]) -> MediaPlan:
    sticker = message.get("sticker")
    if isinstance(sticker, dict):
        return _telegram_sticker_plan(sticker)

    photo = message.get("photo")
    if isinstance(photo, list) and photo:
        largest = max(
            (item for item in photo if isinstance(item, dict)),
            key=_telegram_photo_score,
            default=None,
        )
        if largest is not None:
            return _telegram_file_plan(
                largest,
                kind=MediaKind.IMAGE,
                text_hint="[photo]",
                filename="photo.jpg",
                mime_type="image/jpeg",
            )

    animation = message.get("animation")
    if isinstance(animation, dict):
        return _telegram_animation_plan(animation)

    for key, kind, text_hint, fallback_name, fallback_mime in (
        ("video_note", MediaKind.VIDEO, "[video note]", "video_note.mp4", "video/mp4"),
        ("video", MediaKind.VIDEO, "[video]", "video.mp4", "video/mp4"),
        (
            "document",
            MediaKind.DOCUMENT,
            "[document]",
            "document.bin",
            None,
        ),
        ("audio", MediaKind.AUDIO, "[audio]", "audio.bin", None),
        ("voice", MediaKind.VOICE, "[voice]", "voice.ogg", "audio/ogg"),
    ):
        item = message.get(key)
        if isinstance(item, dict):
            return _telegram_file_plan(
                item,
                kind=kind,
                text_hint=text_hint,
                filename=str(item.get("file_name") or fallback_name),
                mime_type=_string_value(item.get("mime_type")) or fallback_mime,
            )

    return MediaPlan(False)


def _telegram_animation_plan(animation: dict[str, Any]) -> MediaPlan:
    mime_type = _string_value(animation.get("mime_type"))
    filename = _string_value(animation.get("file_name"))
    is_gif = _is_gif_media(mime_type=mime_type, filename=filename, url=None)
    return _telegram_file_plan(
        animation,
        kind=MediaKind.IMAGE if is_gif else MediaKind.VIDEO,
        text_hint="[gif]" if is_gif else "[animation]",
        filename=filename or ("animation.gif" if is_gif else "animation.mp4"),
        mime_type=mime_type or ("image/gif" if is_gif else "video/mp4"),
        presentation=MediaPresentation.ANIMATION,
    )


def _telegram_sticker_plan(sticker: dict[str, Any]) -> MediaPlan:
    if sticker.get("is_animated"):
        return _telegram_file_plan(
            sticker,
            kind=MediaKind.IMAGE,
            text_hint="[animated sticker]",
            filename="sticker.tgs",
            mime_type="application/x-tgsticker",
            sticker_variant="animated_tgs",
            presentation=MediaPresentation.ANIMATION,
        )

    if sticker.get("is_video"):
        return _telegram_file_plan(
            sticker,
            kind=MediaKind.VIDEO,
            text_hint="[video sticker]",
            filename="sticker.webm",
            mime_type="video/webm",
            sticker_variant="video",
        )

    return _telegram_file_plan(
        sticker,
        kind=MediaKind.IMAGE,
        text_hint="[sticker]",
        filename="sticker.webp",
        mime_type="image/webp",
        sticker_variant="static",
    )


def _telegram_file_plan(
    item: dict[str, Any],
    *,
    kind: MediaKind,
    text_hint: str,
    filename: str,
    mime_type: str | None,
    sticker_variant: str | None = None,
    presentation: MediaPresentation | None = None,
) -> MediaPlan:
    file_id = _string_value(item.get("file_id"))
    file_unique_id = _string_value(item.get("file_unique_id"))
    file_size = _int_value(item.get("file_size"))
    if file_id is None:
        return MediaPlan(False, kind.value, text_hint)
    if _is_oversize_telegram(file_size):
        oversize_hint = _telegram_oversize_hint(kind)
        telegram_media_oversize_total.labels(kind.value).inc()
        logger.info(
            "Telegram media exceeds bot download limit kind=%s size=%s limit=%s",
            kind.value,
            file_size,
            TELEGRAM_DOWNLOAD_LIMIT_BYTES,
        )
        return MediaPlan(False, kind.value, oversize_hint)

    resolved_name = _safe_filename(filename, kind)
    return MediaPlan(
        True,
        kind.value,
        text_hint,
        {
            "source_platform": Platform.TELEGRAM.value,
            "kind": kind.value,
            "placeholder": text_hint,
            "filename": resolved_name,
            "mime_type": mime_type,
            "size": file_size,
            "sticker_variant": sticker_variant,
            "presentation": presentation.value if presentation is not None else None,
            "identity": _telegram_media_identity(kind, file_unique_id or file_id),
            "source": {
                "file_id": file_id,
                "file_unique_id": file_unique_id,
            },
        },
    )


def _max_media_plan(message: dict[str, Any]) -> MediaPlan:
    body = message.get("body") if isinstance(message.get("body"), dict) else message
    attachments = body.get("attachments") if isinstance(body, dict) else None
    if not isinstance(attachments, list) or not attachments:
        return MediaPlan(False)

    first = next((item for item in attachments if isinstance(item, dict)), None)
    if first is None:
        return MediaPlan(False)

    raw_type = _string_value(first.get("type")) or "attachment"
    attachment_payload = _attachment_payload(first)
    raw_urls_value = first.get("urls")
    raw_urls: dict[str, Any] = (
        raw_urls_value if isinstance(raw_urls_value, dict) else {}
    )
    download_url = _pick_best_video_url(raw_urls) or _string_value(
        attachment_payload.get("url")
    )
    token = _string_value(attachment_payload.get("token")) or _string_value(
        first.get("token")
    )
    thumbnail_value = first.get("thumbnail")
    thumbnail: dict[str, Any] = (
        thumbnail_value if isinstance(thumbnail_value, dict) else {}
    )
    filename = _string_value(first.get("filename")) or _string_value(
        attachment_payload.get("filename")
    )
    file_size = _int_value(first.get("size")) or _int_value(
        attachment_payload.get("size")
    )
    mime_type = _string_value(first.get("mime_type")) or _string_value(
        attachment_payload.get("mime_type")
    )

    if raw_type == "image":
        photo_id = _scalar_string(
            attachment_payload.get("photo_id") or first.get("photo_id")
        )
        is_gif = _is_gif_media(
            mime_type=mime_type,
            filename=filename,
            url=download_url,
        )
        return _max_attachment_plan(
            kind=MediaKind.IMAGE,
            text_hint="[gif]" if is_gif else "[photo]",
            download_url=download_url,
            token=token,
            filename=filename
            or _filename_from_url(
                download_url,
                "animation.gif" if is_gif else "photo.jpg",
            ),
            mime_type=mime_type or ("image/gif" if is_gif else "image/jpeg"),
            identity=_max_media_identity(
                raw_type=raw_type,
                stable_object_id=f"photo_id:{photo_id}" if photo_id else None,
                download_url=download_url,
                filename=filename,
                mime_type=mime_type or ("image/gif" if is_gif else "image/jpeg"),
                presentation=MediaPresentation.ANIMATION if is_gif else None,
            ),
            presentation=MediaPresentation.ANIMATION if is_gif else None,
            size=file_size,
            extra_source={"photo_id": photo_id} if photo_id else None,
        )
    if raw_type == "video":
        return _max_attachment_plan(
            kind=MediaKind.VIDEO,
            text_hint="[video]",
            download_url=download_url,
            token=token,
            filename=filename or _filename_from_url(download_url, "video.mp4"),
            mime_type=mime_type or "video/mp4",
            identity=_max_media_identity(
                raw_type=raw_type,
                stable_object_id=_max_stable_object_id(first, attachment_payload),
                download_url=download_url,
                filename=filename,
                mime_type=mime_type or "video/mp4",
            ),
            size=file_size,
            extra_source={
                "video_urls": raw_urls,
                "thumbnail_url": _string_value(thumbnail.get("url")),
            },
        )
    if raw_type == "file":
        return _max_attachment_plan(
            kind=MediaKind.DOCUMENT,
            text_hint="[document]",
            download_url=download_url,
            token=token,
            filename=filename or _filename_from_url(download_url, "document.bin"),
            mime_type=mime_type,
            identity=_max_media_identity(
                raw_type=raw_type,
                stable_object_id=_max_stable_object_id(first, attachment_payload),
                download_url=download_url,
                filename=filename,
                mime_type=mime_type,
            ),
            size=file_size,
        )
    if raw_type == "audio":
        return _max_attachment_plan(
            kind=MediaKind.AUDIO,
            text_hint="[audio]",
            download_url=download_url,
            token=token,
            filename=filename or _filename_from_url(download_url, "audio.bin"),
            mime_type=mime_type,
            identity=_max_media_identity(
                raw_type=raw_type,
                stable_object_id=_max_stable_object_id(first, attachment_payload),
                download_url=download_url,
                filename=filename,
                mime_type=mime_type,
            ),
            size=file_size,
        )
    if raw_type == "sticker":
        stable_object_id = _max_stable_object_id(first, attachment_payload)
        return _max_attachment_plan(
            kind=MediaKind.IMAGE,
            text_hint="[sticker]",
            download_url=download_url,
            token=token,
            filename=filename or _filename_from_url(download_url, "sticker.webp"),
            mime_type=_string_value(first.get("mime_type")) or "image/webp",
            identity=_max_media_identity(
                raw_type=raw_type,
                stable_object_id=stable_object_id,
                download_url=download_url,
                filename=filename,
                mime_type=_string_value(first.get("mime_type")) or "image/webp",
                sticker_variant="static",
            ),
            sticker_variant="static",
        )
    return MediaPlan(False, text_hint=f"[{raw_type}]")


def _max_attachment_plan(
    *,
    kind: MediaKind,
    text_hint: str,
    download_url: str | None,
    token: str | None,
    filename: str,
    mime_type: str | None,
    identity: str,
    size: int | None = None,
    sticker_variant: str | None = None,
    presentation: MediaPresentation | None = None,
    extra_source: dict[str, Any] | None = None,
) -> MediaPlan:
    if download_url is None and kind != MediaKind.VIDEO:
        return MediaPlan(False, kind.value, text_hint)
    if download_url is None and token is None:
        return MediaPlan(False, kind.value, text_hint)

    source = {
        "url": download_url,
        "token": token,
    }
    if extra_source:
        source.update(extra_source)

    return MediaPlan(
        True,
        kind.value,
        text_hint,
        {
            "source_platform": Platform.MAX.value,
            "kind": kind.value,
            "placeholder": text_hint,
            "filename": _safe_filename(filename, kind),
            "mime_type": mime_type,
            "size": size,
            "sticker_variant": sticker_variant,
            "presentation": presentation.value if presentation is not None else None,
            "identity": identity,
            "source": source,
        },
    )


def resolve_media_identity(
    media: dict[str, Any] | None,
    *,
    raw_message: dict[str, Any] | None = None,
) -> str | None:
    if not isinstance(media, dict):
        return None
    identity = _string_value(media.get("identity"))
    if identity is not None:
        return identity
    source_platform = _source_platform(media)
    if raw_message is not None and source_platform is not None:
        plan = build_media_plan(source_platform, raw_message)
        if isinstance(plan.payload, dict):
            rebuilt_identity = _string_value(plan.payload.get("identity"))
            if rebuilt_identity is not None:
                return rebuilt_identity
    return _string_value(media.get("signature"))


def _telegram_photo_score(item: dict[str, Any]) -> tuple[int, int]:
    return (
        int(item.get("file_size") or 0),
        int(item.get("width") or 0) * int(item.get("height") or 0),
    )


def _attachment_payload(attachment: dict[str, Any]) -> dict[str, Any]:
    payload = attachment.get("payload")
    return payload if isinstance(payload, dict) else attachment


def _pick_best_video_url(urls: dict[str, Any]) -> str | None:
    for key in VIDEO_URL_ORDER:
        value = _string_value(urls.get(key))
        if value:
            return value
    return None


def _safe_filename(filename: str, kind: MediaKind) -> str:
    candidate = Path(filename).name or _default_filename(kind)
    if "." in candidate:
        return candidate
    default = _default_filename(kind)
    return f"{candidate}{Path(default).suffix}"


def _default_filename(kind: MediaKind) -> str:
    if kind == MediaKind.IMAGE:
        return "image.bin"
    if kind == MediaKind.VIDEO:
        return "video.bin"
    if kind == MediaKind.AUDIO:
        return "audio.bin"
    if kind == MediaKind.VOICE:
        return "voice.ogg"
    return "document.bin"


def _filename_from_url(url: str | None, fallback: str) -> str:
    if not url:
        return fallback
    path = urlparse(url).path
    name = Path(path).name
    return name or fallback


def _telegram_media_identity(kind: MediaKind, stable_id: str) -> str:
    return f"{Platform.TELEGRAM.value}:{kind.value}:id:{stable_id}"


def _max_media_identity(
    *,
    raw_type: str,
    stable_object_id: str | None,
    download_url: str | None,
    filename: str | None,
    mime_type: str | None,
    presentation: MediaPresentation | None = None,
    sticker_variant: str | None = None,
) -> str:
    if stable_object_id is not None:
        return f"{Platform.MAX.value}:{raw_type}:id:{stable_object_id}"
    fingerprint = {
        "type": raw_type,
        "url_host": _normalized_url_host(download_url),
        "url_path": _normalized_url_path(download_url),
        "filename": Path(filename).name if filename else None,
        "mime_type": mime_type.casefold() if mime_type else None,
        "presentation": presentation.value if presentation is not None else None,
        "sticker_variant": sticker_variant,
    }
    return f"{Platform.MAX.value}:{raw_type}:path:{stable_json_hash(fingerprint)}"


def _max_stable_object_id(
    attachment: dict[str, Any],
    attachment_payload: dict[str, Any],
) -> str | None:
    for key in (
        "photo_id",
        "video_id",
        "file_id",
        "audio_id",
        "voice_id",
        "sticker_id",
        "id",
        "code",
    ):
        value = _scalar_string(attachment_payload.get(key) or attachment.get(key))
        if value is not None:
            return f"{key}:{value}"
    return None


def _normalized_url_host(url: str | None) -> str | None:
    if not url:
        return None
    normalized = urlparse(url).netloc.strip().casefold()
    return normalized or None


def _normalized_url_path(url: str | None) -> str | None:
    if not url:
        return None
    normalized = urlparse(url).path.strip()
    return normalized or None


def _is_gif_media(
    *,
    mime_type: str | None,
    filename: str | None,
    url: str | None,
) -> bool:
    if mime_type is not None and mime_type.casefold() == "image/gif":
        return True
    if _has_gif_extension(filename):
        return True
    if url is None:
        return False
    return _has_gif_extension(urlparse(url).path)


def _has_gif_extension(value: str | None) -> bool:
    if not value:
        return False
    return Path(value).suffix.casefold() == ".gif"


def _string_value(value: object) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _int_value(value: object) -> int | None:
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


def _scalar_string(value: object) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    return _string_value(value)


def _source_platform(media: dict[str, Any]) -> Platform | None:
    raw = _string_value(media.get("source_platform"))
    if raw is None:
        return None
    try:
        return Platform(raw)
    except ValueError:
        return None


def _is_oversize_telegram(size: int | None) -> bool:
    return size is not None and size > TELEGRAM_DOWNLOAD_LIMIT_BYTES


def _telegram_oversize_hint(kind: MediaKind) -> str:
    return (
        f"[{kind.value} unavailable: exceeds Telegram bot 20 MB download limit]"
    )


def destination_media_upload_limit_bytes(
    platform: Platform,
    media: LocalMediaFile,
) -> int:
    return destination_upload_limit_bytes(
        platform,
        media.kind,
        presentation=media.presentation,
    )


def destination_upload_limit_bytes(
    platform: Platform,
    kind: MediaKind,
    *,
    presentation: MediaPresentation | None = None,
) -> int:
    if platform == Platform.TELEGRAM:
        if presentation == MediaPresentation.ANIMATION:
            return TELEGRAM_UPLOAD_FILE_LIMIT_BYTES
        if kind == MediaKind.IMAGE:
            return TELEGRAM_UPLOAD_PHOTO_LIMIT_BYTES
        return TELEGRAM_UPLOAD_FILE_LIMIT_BYTES
    return MAX_UPLOAD_LIMIT_BYTES


def destination_upload_oversize_hint(
    platform: Platform,
    kind: MediaKind,
    *,
    presentation: MediaPresentation | None = None,
) -> str:
    limit_bytes = destination_upload_limit_bytes(
        platform,
        kind,
        presentation=presentation,
    )
    platform_name = "Telegram" if platform == Platform.TELEGRAM else "MAX"
    limit_mb = limit_bytes // (1024 * 1024)
    return (
        f"[{kind.value} unavailable: exceeds {platform_name} "
        f"{limit_mb} MB upload limit]"
    )
