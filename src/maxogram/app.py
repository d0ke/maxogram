from __future__ import annotations

import asyncio
import logging
import signal
import uuid
from contextlib import suppress
from typing import Protocol

from .config import AppSettings
from .db.repositories import Repository
from .db.session import Database
from .domain import Platform
from .platforms.base import PlatformClient
from .platforms.max import MaxClient
from .platforms.telegram import TelegramClient
from .runtime_resilience import (
    RuntimeBackoffState,
    is_transient_db_error,
    wait_or_stop,
)
from .services.commands import CommandProcessor
from .workers.delivery import DeliveryWorker
from .workers.normalizer import NormalizerWorker
from .workers.pollers import PollerWorker
from .workers.reconciliation import ReconciliationWorker

logger = logging.getLogger(__name__)


class Worker(Protocol):
    name: str

    async def run(self) -> None: ...


class MaxogramApp:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.database = Database(settings.db.sqlalchemy_url())
        self.stop_event = asyncio.Event()

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.stop_event.set)

        async with self.database:
            bootstrap = await self._bootstrap_database_until_ready()
            if bootstrap is None:
                return
            telegram_bot_id, max_bot_id = bootstrap

            telegram = TelegramClient(self.settings.telegram_token)
            max_client = MaxClient(self.settings.max_token)
            clients: dict[Platform, PlatformClient] = {
                Platform.TELEGRAM: telegram,
                Platform.MAX: max_client,
            }
            workers: list[Worker] = [
                PollerWorker(
                    database=self.database,
                    platform=Platform.TELEGRAM,
                    bot_id=telegram_bot_id,
                    client=telegram,
                    stop_event=self.stop_event,
                    limit=self.settings.poll_limit,
                    timeout=self.settings.telegram_poll_timeout,
                    idle_seconds=self.settings.worker_idle_seconds,
                ),
                PollerWorker(
                    database=self.database,
                    platform=Platform.MAX,
                    bot_id=max_bot_id,
                    client=max_client,
                    stop_event=self.stop_event,
                    limit=self.settings.poll_limit,
                    timeout=self.settings.max_poll_timeout,
                    idle_seconds=self.settings.worker_idle_seconds,
                ),
                NormalizerWorker(
                    database=self.database,
                    clients=clients,
                    command_processor=CommandProcessor(),
                    stop_event=self.stop_event,
                    idle_seconds=self.settings.worker_idle_seconds,
                ),
                DeliveryWorker(
                    database=self.database,
                    clients=clients,
                    stop_event=self.stop_event,
                    lease_seconds=self.settings.outbox_lease_seconds,
                    idle_seconds=self.settings.worker_idle_seconds,
                    root_dir=self.settings.root_dir,
                ),
                ReconciliationWorker(
                    database=self.database,
                    stop_event=self.stop_event,
                    idle_seconds=max(self.settings.worker_idle_seconds, 5.0),
                ),
            ]

            tasks = [
                asyncio.create_task(worker.run(), name=worker.name)
                for worker in workers
            ]
            try:
                await self.stop_event.wait()
            finally:
                logger.info("Shutting down workers")
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await telegram.close()
                await max_client.close()

    async def _bootstrap_database_until_ready(
        self,
    ) -> tuple[uuid.UUID, uuid.UUID] | None:
        backoff = RuntimeBackoffState()
        while not self.stop_event.is_set():
            try:
                bootstrap = await self._bootstrap_database_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not is_transient_db_error(exc):
                    raise
                delay_seconds = backoff.next_delay_seconds()
                logger.warning(
                    "Database bootstrap failed attempt=%s error=%s retry_in=%.0fs: %s",
                    backoff.attempts,
                    exc.__class__.__name__,
                    delay_seconds,
                    exc,
                )
                if await wait_or_stop(self.stop_event, delay_seconds):
                    return None
            else:
                recovered_attempts = backoff.clear()
                if recovered_attempts:
                    logger.info(
                        "Database bootstrap recovered after %s temporary failure(s)",
                        recovered_attempts,
                    )
                return bootstrap
        return None

    async def _bootstrap_database_once(self) -> tuple[uuid.UUID, uuid.UUID]:
        async with self.database.session() as session:
            repo = Repository(session)
            telegram_bot_id = await repo.ensure_bot_credential(Platform.TELEGRAM)
            max_bot_id = await repo.ensure_bot_credential(Platform.MAX)
            await repo.ensure_proxy_profile(Platform.TELEGRAM)
            await repo.ensure_proxy_profile(Platform.MAX)
            await session.commit()
        return telegram_bot_id, max_bot_id
