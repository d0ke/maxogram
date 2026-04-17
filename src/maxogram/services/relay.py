from __future__ import annotations

import asyncio
import gzip
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from maxogram.domain import LocalMediaFile, MediaKind, MediaPresentation, Platform
from maxogram.platforms.base import PlatformClient, PlatformDeliveryError
from maxogram.services.dedup import stable_json_hash
from maxogram.services.media import resolve_media_identity

logger = logging.getLogger(__name__)

ANIMATED_STICKER_CACHE_RETENTION = timedelta(days=90)
ANIMATED_STICKER_CACHE_SWEEP_INTERVAL = timedelta(days=1)
TGS_TO_GIF_PROFILE_VERSION = "tgs_to_gif_v1"


def media_cache_dir(root_dir: Path) -> Path:
    return root_dir / "temp" / "media_cache"


def animated_sticker_cache_dir(root_dir: Path) -> Path:
    return root_dir / "temp" / "animated_sticker_cache"


async def materialize_media(
    *,
    clients: dict[Platform, PlatformClient],
    media: dict[str, Any],
    root_dir: Path,
) -> LocalMediaFile | None:
    source_platform_raw = media.get("source_platform")
    if source_platform_raw is None:
        raise PlatformDeliveryError(
            "Media payload is missing source_platform",
            retryable=False,
            code="invalid_media_payload",
        )
    try:
        source_platform = Platform(str(source_platform_raw))
    except ValueError as exc:
        raise PlatformDeliveryError(
            f"Unsupported media source platform: {source_platform_raw}",
            retryable=False,
            code="invalid_media_payload",
        ) from exc
    if _is_telegram_animated_tgs(media, source_platform):
        return await _materialize_telegram_animated_sticker(
            client=clients[source_platform],
            media=media,
            root_dir=root_dir,
        )
    destination_dir = media_cache_dir(root_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    return await clients[source_platform].download_media(media, destination_dir)


def cleanup_local_media(media: LocalMediaFile | None) -> None:
    if media is None or not media.cleanup_after_use:
        return
    if media.path.exists():
        media.path.unlink(missing_ok=True)


def prune_animated_sticker_cache(
    root_dir: Path,
    *,
    now: datetime | None = None,
) -> int:
    cache_dir = animated_sticker_cache_dir(root_dir)
    if not cache_dir.exists():
        return 0
    cutoff = (now or datetime.now(UTC)) - ANIMATED_STICKER_CACHE_RETENTION
    pruned = 0
    for path in cache_dir.iterdir():
        if not path.is_file():
            continue
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        if modified_at >= cutoff:
            continue
        path.unlink(missing_ok=True)
        pruned += 1
    return pruned


async def _materialize_telegram_animated_sticker(
    *,
    client: PlatformClient,
    media: dict[str, Any],
    root_dir: Path,
) -> LocalMediaFile | None:
    cache_path = _animated_sticker_cache_path(root_dir, media)
    if cache_path.exists():
        cache_path.touch(exist_ok=True)
        return _cached_animated_sticker_media(media, cache_path)

    download_dir = media_cache_dir(root_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    downloaded_media = await client.download_media(media, download_dir)
    if downloaded_media is None:
        return None

    try:
        await _convert_tgs_to_cached_gif(downloaded_media.path, cache_path)
    except Exception:
        logger.exception(
            "Animated Telegram sticker conversion failed identity=%s",
            _animated_sticker_identity(media),
        )
        return None
    finally:
        cleanup_local_media(downloaded_media)

    cache_path.touch(exist_ok=True)
    return _cached_animated_sticker_media(media, cache_path)


async def _convert_tgs_to_cached_gif(source_path: Path, cache_path: Path) -> None:
    from pyrlottie import LottieFile, convSingleLottie

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    lottie_data = await asyncio.to_thread(_load_tgs_data, source_path)
    json_path, temp_output = _allocate_conversion_paths(cache_path.parent)
    try:
        await asyncio.to_thread(_write_lottie_json, json_path, lottie_data)
        await convSingleLottie(
            lottieFile=LottieFile(str(json_path), data=lottie_data),
            destFiles={str(temp_output)},
        )
        if not temp_output.exists():
            raise RuntimeError("Animated sticker conversion did not produce a GIF")
        await asyncio.to_thread(temp_output.replace, cache_path)
    finally:
        json_path.unlink(missing_ok=True)
        temp_output.unlink(missing_ok=True)


def _is_telegram_animated_tgs(media: dict[str, Any], source_platform: Platform) -> bool:
    return (
        source_platform == Platform.TELEGRAM
        and str(media.get("kind")) == MediaKind.IMAGE.value
        and str(media.get("sticker_variant")) == "animated_tgs"
    )


def _animated_sticker_cache_path(root_dir: Path, media: dict[str, Any]) -> Path:
    fingerprint = {
        "identity": _animated_sticker_identity(media),
        "profile": TGS_TO_GIF_PROFILE_VERSION,
    }
    digest = stable_json_hash(fingerprint)
    return animated_sticker_cache_dir(root_dir) / f"{digest}.gif"


def _animated_sticker_identity(media: dict[str, Any]) -> str:
    identity = resolve_media_identity(media)
    if identity is not None:
        return identity
    source = media.get("source") if isinstance(media.get("source"), dict) else {}
    fallback_source = source if isinstance(source, dict) else {}
    return stable_json_hash(
        {
            "source_platform": media.get("source_platform"),
            "kind": media.get("kind"),
            "sticker_variant": media.get("sticker_variant"),
            "file_id": fallback_source.get("file_id"),
            "file_unique_id": fallback_source.get("file_unique_id"),
        }
    )


def _cached_animated_sticker_media(
    media: dict[str, Any],
    cache_path: Path,
) -> LocalMediaFile:
    return LocalMediaFile(
        kind=MediaKind.IMAGE,
        path=cache_path,
        filename=_animated_sticker_filename(media),
        mime_type="image/gif",
        sticker_variant="animated_tgs",
        presentation=MediaPresentation.ANIMATION,
        cleanup_after_use=False,
    )


def _animated_sticker_filename(media: dict[str, Any]) -> str:
    original_name = Path(str(media.get("filename") or "animated-sticker.tgs")).stem
    return f"{original_name or 'animated-sticker'}.gif"


def _load_tgs_data(source_path: Path) -> dict[str, Any]:
    with gzip.open(source_path, mode="rt", encoding="utf-8") as archive:
        payload = json.load(archive)
    if not isinstance(payload, dict):
        raise ValueError("Animated sticker payload must be a JSON object")
    return payload


def _allocate_conversion_paths(cache_dir: Path) -> tuple[Path, Path]:
    with NamedTemporaryFile(
        dir=cache_dir,
        suffix=".json",
        delete=False,
    ) as json_file:
        json_path = Path(json_file.name)
    with NamedTemporaryFile(
        dir=cache_dir,
        suffix=".gif",
        delete=False,
    ) as gif_file:
        gif_path = Path(gif_file.name)
    return json_path, gif_path


def _write_lottie_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
