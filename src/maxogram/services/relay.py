from __future__ import annotations

from pathlib import Path
from typing import Any

from maxogram.domain import LocalMediaFile, Platform
from maxogram.platforms.base import PlatformClient, PlatformDeliveryError


def media_cache_dir(root_dir: Path) -> Path:
    return root_dir / "temp" / "media_cache"


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
    destination_dir = media_cache_dir(root_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    return await clients[source_platform].download_media(media, destination_dir)


def cleanup_local_media(media: LocalMediaFile | None) -> None:
    if media is None:
        return
    if media.path.exists():
        media.path.unlink(missing_ok=True)
