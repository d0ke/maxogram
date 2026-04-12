from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from maxogram.domain import LocalMediaFile, PollBatch, SendResult


class PlatformDeliveryError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        code: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.code = code
        self.http_status = http_status


@dataclass(slots=True)
class RateLimiter:
    max_per_second: float
    _last_call: float = 0.0

    async def wait(self) -> None:
        import asyncio
        import time

        now = time.monotonic()
        min_interval = 1.0 / self.max_per_second
        delay = self._last_call + min_interval - now
        if delay > 0:
            await asyncio.sleep(delay)
        self._last_call = time.monotonic()


class PlatformClient(Protocol):
    async def poll_updates(
        self,
        cursor: int | None,
        *,
        limit: int,
        poll_timeout: int,
    ) -> PollBatch: ...

    async def send_text(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
    ) -> SendResult: ...

    async def send_message(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
        media: LocalMediaFile | None = None,
    ) -> SendResult: ...

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        has_media: bool = False,
        replacement_media: LocalMediaFile | None = None,
    ) -> None: ...

    async def delete_message(self, chat_id: str, message_id: str) -> None: ...

    async def download_media(
        self,
        media: dict[str, object],
        destination_dir: Path,
    ) -> LocalMediaFile | None: ...

    async def is_admin(self, chat_id: str, user_id: str) -> bool: ...

    async def close(self) -> None: ...
