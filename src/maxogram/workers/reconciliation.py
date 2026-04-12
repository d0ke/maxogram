from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from maxogram.db.repositories import Repository
from maxogram.db.session import Database
from maxogram.domain import OutboxAction, Platform
from maxogram.services.dedup import outbox_dedup_key, partition_key

logger = logging.getLogger(__name__)


class ReconciliationWorker:
    name = "reconciliation"

    def __init__(
        self,
        *,
        database: Database,
        stop_event: asyncio.Event,
        idle_seconds: float,
        pending_batch_size: int = 50,
        pending_retry_seconds: int = 5,
    ) -> None:
        self.database = database
        self.stop_event = stop_event
        self.idle_seconds = idle_seconds
        self.pending_batch_size = pending_batch_size
        self.pending_retry_seconds = pending_retry_seconds

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Reconciliation failed")
            await asyncio.sleep(self.idle_seconds)

    async def run_once(self) -> tuple[int, int, int]:
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                reset = await repo.reset_expired_inflight()
                replayed = await self._requeue_pending(repo)
                expired = await repo.expire_pending_mutations()
        if reset or replayed or expired:
            logger.info(
                "Reconciled reset=%s replayed=%s expired=%s",
                reset,
                replayed,
                expired,
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

    def _next_pending_attempt_at(self, pending: Any) -> datetime:
        return min(
            datetime.now(UTC) + timedelta(seconds=self.pending_retry_seconds),
            pending.expires_at,
        )
