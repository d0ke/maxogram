from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from maxogram.db.repositories import Repository
from maxogram.db.session import Database
from maxogram.domain import OutboxAction, Platform
from maxogram.runtime_resilience import (
    RuntimeBackoffState,
    is_transient_db_error,
    wait_or_stop,
)
from maxogram.services.dedup import outbox_dedup_key, partition_key
from maxogram.services.relay import (
    ANIMATED_STICKER_CACHE_SWEEP_INTERVAL,
    prune_animated_sticker_cache,
)

logger = logging.getLogger(__name__)


class ReconciliationWorker:
    name = "reconciliation"

    def __init__(
        self,
        *,
        database: Database,
        stop_event: asyncio.Event,
        idle_seconds: float,
        root_dir: Path,
        pending_batch_size: int = 50,
        pending_retry_seconds: int = 5,
    ) -> None:
        self.database = database
        self.stop_event = stop_event
        self.idle_seconds = idle_seconds
        self.root_dir = root_dir
        self.pending_batch_size = pending_batch_size
        self.pending_retry_seconds = pending_retry_seconds
        self._retry_backoff = RuntimeBackoffState()
        self._last_cache_prune_at: datetime | None = None

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.run_once()
                self._log_recovery_if_needed()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_transient_db_error(exc):
                    if await self._wait_after_retryable_failure(exc):
                        return
                    continue
                self._retry_backoff.clear()
                logger.exception("Reconciliation failed")
            if await wait_or_stop(self.stop_event, self.idle_seconds):
                return

    async def run_once(self) -> tuple[int, int, int]:
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                reset = await repo.reset_expired_inflight()
                replayed = await self._requeue_pending(repo)
                expired = await repo.expire_pending_mutations()
        pruned = await self._prune_animated_sticker_cache_if_due()
        if reset or replayed or expired or pruned:
            logger.info(
                "Reconciled reset=%s replayed=%s expired=%s pruned=%s",
                reset,
                replayed,
                expired,
                pruned,
            )
        return reset, replayed, expired

    async def _requeue_pending(self, repo: Repository) -> int:
        replayed = 0
        rows = await repo.claim_pending_mutations(self.pending_batch_size)
        for pending in rows:
            mapping = await repo.find_mapping_by_source(
                pending.bridge_id,
                pending.src_platform,
                pending.src_chat_id,
                pending.src_message_id,
            )
            if mapping is None:
                await repo.reschedule_pending_mutation(
                    pending,
                    next_attempt_at=self._next_pending_attempt_at(pending),
                )
                continue

            src_event_id = await repo.find_canonical_event_id_by_dedup_key(
                pending.dedup_key
            )
            dst_payload = await self._resolve_pending_destination(repo, pending)
            if src_event_id is None or dst_payload is None:
                await repo.reschedule_pending_mutation(
                    pending,
                    next_attempt_at=self._next_pending_attempt_at(pending),
                )
                continue

            action = OutboxAction(pending.mutation_type)
            payload = {
                **pending.payload,
                "dst": dst_payload,
                "dst_message_id": mapping.dst_message_id,
            }
            dst_message_ids = await repo.list_destination_message_ids(
                pending.bridge_id,
                pending.src_platform,
                pending.src_chat_id,
                pending.src_message_id,
            )
            if dst_message_ids:
                payload["dst_message_ids"] = dst_message_ids
            version = payload.get("version")
            version_key = version if isinstance(version, str | int) else None
            dst_platform = Platform(str(dst_payload["platform"]))
            await repo.enqueue_outbox(
                bridge_id=pending.bridge_id,
                dedup_key=outbox_dedup_key(
                    pending.bridge_id,
                    pending.src_platform,
                    pending.src_chat_id,
                    pending.src_message_id,
                    dst_platform,
                    action,
                    version_key,
                ),
                src_event_id=src_event_id,
                dst_platform=dst_platform,
                action=action,
                partition_key=partition_key(
                    pending.bridge_id,
                    pending.src_platform,
                    dst_platform,
                ),
                task=payload,
            )
            await repo.mark_pending_mutation_done(pending)
            replayed += 1
        return replayed

    async def _resolve_pending_destination(
        self,
        repo: Repository,
        pending: Any,
    ) -> dict[str, str] | None:
        dst = pending.payload.get("dst") if isinstance(pending.payload, dict) else None
        if isinstance(dst, dict):
            platform = dst.get("platform")
            chat_id = dst.get("chat_id")
            if platform is not None and chat_id is not None:
                return {"platform": str(platform), "chat_id": str(chat_id)}

        bridge_chat = await repo.find_other_chat(
            pending.bridge_id,
            pending.src_platform,
        )
        if bridge_chat is None:
            return None
        return {"platform": bridge_chat.platform.value, "chat_id": bridge_chat.chat_id}

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

    def _next_pending_attempt_at(self, pending: Any) -> datetime:
        return min(
            datetime.now(UTC) + timedelta(seconds=self.pending_retry_seconds),
            pending.expires_at,
        )

    async def _prune_animated_sticker_cache_if_due(self) -> int:
        now = datetime.now(UTC)
        if self._last_cache_prune_at is not None and (
            now - self._last_cache_prune_at
        ) < ANIMATED_STICKER_CACHE_SWEEP_INTERVAL:
            return 0
        self._last_cache_prune_at = now
        try:
            return await asyncio.to_thread(
                prune_animated_sticker_cache,
                self.root_dir,
                now=now,
            )
        except Exception:
            logger.exception("Animated sticker cache pruning failed")
            return 0
