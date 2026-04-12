from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from maxogram.domain import Platform, TaskStatus
from maxogram.workers.reconciliation import ReconciliationWorker


class FakeRepository:
    def __init__(
        self,
        *,
        pending_rows: list[Any],
        mapping: Any | None,
        event_id: uuid.UUID | None,
        dst_chat: Any | None = None,
    ) -> None:
        self.pending_rows = pending_rows
        self.mapping = mapping
        self.event_id = event_id
        self.dst_chat = dst_chat or SimpleNamespace(
            platform=Platform.TELEGRAM,
            chat_id="-100",
        )
        self.enqueued: list[dict[str, Any]] = []
        self.done: list[uuid.UUID] = []
        self.rescheduled: list[datetime] = []

    async def claim_pending_mutations(self, limit: int) -> list[Any]:
        return self.pending_rows[:limit]

    async def find_mapping_by_source(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> Any | None:
        _ = bridge_id, src_platform, src_chat_id, src_message_id
        return self.mapping

    async def find_canonical_event_id_by_dedup_key(
        self, dedup_key: str
    ) -> uuid.UUID | None:
        _ = dedup_key
        return self.event_id

    async def find_other_chat(
        self,
        bridge_id: uuid.UUID,
        source_platform: Platform,
    ) -> Any:
        _ = bridge_id, source_platform
        return self.dst_chat

    async def enqueue_outbox(self, **kwargs: Any) -> uuid.UUID:
        self.enqueued.append(kwargs)
        return uuid.uuid4()

    async def mark_pending_mutation_done(self, pending: Any) -> None:
        pending.status = TaskStatus.DONE
        self.done.append(pending.pending_id)

    async def reschedule_pending_mutation(
        self,
        pending: Any,
        *,
        next_attempt_at: datetime,
    ) -> None:
        pending.next_attempt_at = next_attempt_at
        self.rescheduled.append(next_attempt_at)


def make_worker() -> ReconciliationWorker:
    return ReconciliationWorker(
        database=None,  # type: ignore[arg-type]
        stop_event=asyncio.Event(),
        idle_seconds=0,
    )


def make_pending(*, payload: dict[str, Any]) -> Any:
    return SimpleNamespace(
        pending_id=uuid.uuid4(),
        bridge_id=uuid.uuid4(),
        dedup_key="max:100:mid-1:message.edited:12",
        src_platform=Platform.MAX,
        src_chat_id="100",
        src_message_id="mid-1",
        mutation_type="edit",
        payload=payload,
        status=TaskStatus.RETRY_WAIT,
        next_attempt_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=3),
    )


def make_mapping(*, dst_message_id: str) -> Any:
    return cast(Any, SimpleNamespace(dst_message_id=dst_message_id))


@pytest.mark.asyncio
async def test_requeue_pending_mutation_enqueues_outbox_when_mapping_exists():
    pending = make_pending(
        payload={
            "src": {"platform": "max", "chat_id": "100", "message_id": "mid-1"},
            "text": "Alice: updated",
            "version": 12,
        }
    )
    repo = FakeRepository(
        pending_rows=[pending],
        mapping=make_mapping(dst_message_id="tg-msg-1"),
        event_id=uuid.uuid4(),
    )
    worker = make_worker()

    replayed = await worker._requeue_pending(repo)  # type: ignore[arg-type]

    assert replayed == 1
    assert repo.rescheduled == []
    assert repo.done == [pending.pending_id]
    assert len(repo.enqueued) == 1
    queued = repo.enqueued[0]
    assert queued["dst_platform"] == Platform.TELEGRAM
    assert queued["action"].value == "edit"
    task = cast(dict[str, Any], queued["task"])
    assert task["dst"]["platform"] == "telegram"
    assert task["dst"]["chat_id"] == "-100"
    assert task["dst_message_id"] == "tg-msg-1"


@pytest.mark.asyncio
async def test_requeue_pending_mutation_reschedules_when_mapping_is_missing():
    pending = make_pending(
        payload={
            "src": {"platform": "max", "chat_id": "100", "message_id": "mid-1"},
            "text": "Alice: updated",
            "version": 12,
            "dst": {"platform": "telegram", "chat_id": "-100"},
        }
    )
    repo = FakeRepository(
        pending_rows=[pending],
        mapping=None,
        event_id=uuid.uuid4(),
    )
    worker = make_worker()

    replayed = await worker._requeue_pending(repo)  # type: ignore[arg-type]

    assert replayed == 0
    assert repo.enqueued == []
    assert repo.done == []
    assert len(repo.rescheduled) == 1
    assert pending.status == TaskStatus.RETRY_WAIT
