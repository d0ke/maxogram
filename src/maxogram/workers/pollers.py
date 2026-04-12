from __future__ import annotations

import asyncio
import logging
import uuid

from maxogram.db.repositories import Repository
from maxogram.db.session import Database
from maxogram.domain import Platform
from maxogram.metrics import duplicate_update_total
from maxogram.platforms.base import PlatformClient, PlatformDeliveryError
from maxogram.runtime_resilience import (
    RuntimeBackoffState,
    is_transient_db_error,
    wait_or_stop,
)

logger = logging.getLogger(__name__)


class PollerWorker:
    def __init__(
        self,
        *,
        database: Database,
        platform: Platform,
        bot_id: uuid.UUID,
        client: PlatformClient,
        stop_event: asyncio.Event,
        limit: int,
        timeout: int,
        idle_seconds: float,
    ) -> None:
        self.database = database
        self.platform = platform
        self.bot_id = bot_id
        self.client = client
        self.stop_event = stop_event
        self.limit = limit
        self.timeout = timeout
        self.idle_seconds = idle_seconds
        self.name = f"{platform.value}-poller"
        self._retry_backoff = RuntimeBackoffState()

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except PlatformDeliveryError as exc:
                if exc.retryable:
                    if await self._wait_after_retryable_failure(exc):
                        return
                    continue
                self._retry_backoff.clear()
                logger.warning("%s polling failed: %s", self.name, exc)
                if await wait_or_stop(self.stop_event, self.idle_seconds):
                    return
            except Exception as exc:
                if is_transient_db_error(exc):
                    if await self._wait_after_retryable_failure(exc):
                        return
                    continue
                self._retry_backoff.clear()
                logger.exception("%s crashed during polling", self.name)
                if await wait_or_stop(self.stop_event, self.idle_seconds):
                    return
            else:
                self._log_recovery_if_needed()

    async def run_once(self) -> int:
        async with self.database.session() as session:
            repo = Repository(session)
            cursor = await repo.get_cursor(self.platform, self.bot_id)
        batch = await self.client.poll_updates(
            cursor, limit=self.limit, poll_timeout=self.timeout
        )
        if not batch.updates and batch.next_cursor == cursor:
            return 0

        inserted = 0
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                for update in batch.updates:
                    if await repo.insert_inbox_update(
                        self.platform, self.bot_id, update.update_key, update.raw
                    ):
                        inserted += 1
                    else:
                        if self.platform == Platform.MAX:
                            logger.info(
                                "Dropped duplicate MAX update type=%s key=%s",
                                update.raw.get("update_type") or update.raw.get("type"),
                                update.update_key,
                            )
                        duplicate_update_total.labels(self.platform.value).inc()
                if batch.next_cursor is not None:
                    await repo.upsert_cursor(
                        self.platform, self.bot_id, batch.next_cursor
                    )
        return inserted

    async def _wait_after_retryable_failure(self, exc: BaseException) -> bool:
        delay_seconds = self._retry_backoff.next_delay_seconds()
        logger.warning(
            "%s temporary failure attempt=%s error=%s retry_in=%.0fs: %s",
            self.name,
            self._retry_backoff.attempts,
            exc.__class__.__name__,
            delay_seconds,
            exc,
        )
        return await wait_or_stop(self.stop_event, delay_seconds)

    def _log_recovery_if_needed(self) -> None:
        recovered_attempts = self._retry_backoff.clear()
        if recovered_attempts:
            logger.info(
                "%s recovered after %s temporary failure(s)",
                self.name,
                recovered_attempts,
            )
