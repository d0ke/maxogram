from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from maxogram.domain import Platform, TaskStatus
from maxogram.services.relay import (
    animated_sticker_cache_dir,
    prune_animated_sticker_cache,
)
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


def make_worker(tmp_path: Path) -> ReconciliationWorker:
    return ReconciliationWorker(
        database=None,  # type: ignore[arg-type]
        stop_event=asyncio.Event(),
        idle_seconds=0,
        root_dir=tmp_path,
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
async def test_requeue_pending_mutation_enqueues_outbox_when_mapping_exists(
    tmp_path: Path,
):
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
    worker = make_worker(tmp_path)

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
async def test_requeue_pending_mutation_reschedules_when_mapping_is_missing(
    tmp_path: Path,
):
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
    worker = make_worker(tmp_path)

    replayed = await worker._requeue_pending(repo)  # type: ignore[arg-type]

    assert replayed == 0
    assert repo.enqueued == []
    assert repo.done == []
    assert len(repo.rescheduled) == 1
    assert pending.status == TaskStatus.RETRY_WAIT


def test_prune_animated_sticker_cache_removes_only_stale_files(tmp_path: Path):
    cache_dir = animated_sticker_cache_dir(tmp_path)
    cache_dir.mkdir(parents=True)
    stale_file = cache_dir / "stale.gif"
    fresh_file = cache_dir / "fresh.gif"
    stale_file.write_bytes(b"old")
    fresh_file.write_bytes(b"new")

    stale_timestamp = datetime.now(UTC) - timedelta(days=91)
    fresh_timestamp = datetime.now(UTC) - timedelta(days=5)
    stale_epoch = stale_timestamp.timestamp()
    fresh_epoch = fresh_timestamp.timestamp()
    stale_file.touch()
    fresh_file.touch()
    os.utime(stale_file, (stale_epoch, stale_epoch))
    os.utime(fresh_file, (fresh_epoch, fresh_epoch))

    pruned = prune_animated_sticker_cache(tmp_path, now=datetime.now(UTC))

    assert pruned == 1
    assert not stale_file.exists()
    assert fresh_file.exists()


@pytest.mark.asyncio
async def test_reconciliation_prunes_animated_sticker_cache_only_once_per_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[Path] = []

    class FakeSession:
        async def __aenter__(self) -> "FakeSession":
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            _ = exc_type, exc, tb
            return None

        class _Begin:
            async def __aenter__(self) -> "FakeSession._Begin":
                return self

            async def __aexit__(
                self,
                exc_type: object,
                exc: object,
                tb: object,
            ) -> None:
                _ = exc_type, exc, tb
                return None

        def begin(self) -> "FakeSession._Begin":
            return self._Begin()

    class FakeDatabase:
        def session(self) -> FakeSession:
            return FakeSession()

    class FakeRunOnceRepository:
        def __init__(self, session: FakeSession) -> None:
            _ = session

        async def reset_expired_inflight(self) -> int:
            return 0

        async def claim_pending_mutations(self, limit: int) -> list[Any]:
            _ = limit
            return []

        async def expire_pending_mutations(self) -> int:
            return 0

    def fake_prune(root_dir: Path, *, now: datetime | None = None) -> int:
        _ = now
        calls.append(root_dir)
        return 0

    worker = ReconciliationWorker(
        database=cast(Any, FakeDatabase()),
        stop_event=asyncio.Event(),
        idle_seconds=0,
        root_dir=tmp_path,
    )
    monkeypatch.setattr(
        "maxogram.workers.reconciliation.Repository",
        FakeRunOnceRepository,
    )
    monkeypatch.setattr(
        "maxogram.workers.reconciliation.prune_animated_sticker_cache",
        fake_prune,
    )

    await worker.run_once()
    await worker.run_once()

    assert calls == [tmp_path]
