from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from maxogram.domain import Platform, PollBatch, PollUpdate
from maxogram.metrics import duplicate_update_total
from maxogram.workers import pollers as pollers_module
from maxogram.workers.pollers import PollerWorker


@dataclass
class FakeState:
    cursor: int | None = None
    seen_keys: set[str] = field(default_factory=set)
    inserted_updates: list[dict[str, Any]] = field(default_factory=list)


class FakeSession:
    def __init__(self, state: FakeState) -> None:
        self.state = state

    @asynccontextmanager
    async def begin(self):
        yield self


class FakeDatabase:
    def __init__(self, state: FakeState) -> None:
        self.state = state

    @asynccontextmanager
    async def session(self):
        yield FakeSession(self.state)


class FakeRepository:
    def __init__(self, session: FakeSession) -> None:
        self.state = session.state

    async def get_cursor(self, platform: Platform, bot_id: uuid.UUID) -> int | None:
        _ = platform, bot_id
        return self.state.cursor

    async def insert_inbox_update(
        self,
        platform: Platform,
        bot_id: uuid.UUID,
        update_key: str,
        raw: dict[str, Any],
    ) -> bool:
        _ = platform, bot_id
        if update_key in self.state.seen_keys:
            return False
        self.state.seen_keys.add(update_key)
        self.state.inserted_updates.append(
            {"update_key": update_key, "update_type": raw["update_type"], "raw": raw}
        )
        return True

    async def upsert_cursor(
        self,
        platform: Platform,
        bot_id: uuid.UUID,
        cursor_value: int,
    ) -> None:
        _ = platform, bot_id
        self.state.cursor = cursor_value


class FakeClient:
    def __init__(self, batch: PollBatch) -> None:
        self.batch = batch
        self.calls: list[dict[str, Any]] = []

    async def poll_updates(
        self,
        cursor: int | None,
        *,
        limit: int,
        poll_timeout: int,
    ) -> PollBatch:
        self.calls.append(
            {"cursor": cursor, "limit": limit, "poll_timeout": poll_timeout}
        )
        return self.batch


@pytest.mark.asyncio
async def test_max_poller_keeps_created_and_edited_updates_with_same_ids(
    monkeypatch: pytest.MonkeyPatch,
):
    state = FakeState(cursor=33_113_682)
    created_raw = {
        "update_type": "message_created",
        "timestamp": 1_775_863_528_329,
        "message": {
            "recipient": {"chat_id": 1_000},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {
                "mid": "mid.same",
                "seq": 116_382_992_192_573_216,
                "text": "Test1",
            },
        },
    }
    edited_raw = {
        "update_type": "message_edited",
        "timestamp": 1_775_863_528_329,
        "message": {
            "recipient": {"chat_id": 1_000},
            "sender": {"user_id": 42, "first_name": "Alice"},
            "body": {
                "mid": "mid.same",
                "seq": 116_382_992_192_573_216,
                "text": "Test2",
            },
        },
    }
    client = FakeClient(
        PollBatch(
            updates=[
                PollUpdate("created", created_raw),
                PollUpdate("edited", edited_raw),
            ],
            next_cursor=33_113_684,
        )
    )
    before_duplicates = duplicate_update_total.labels(Platform.MAX.value)._value.get()
    monkeypatch.setattr(pollers_module, "Repository", FakeRepository)
    worker = PollerWorker(
        database=cast(Any, FakeDatabase(state)),
        platform=Platform.MAX,
        bot_id=uuid.uuid4(),
        client=cast(Any, client),
        stop_event=asyncio.Event(),
        limit=100,
        timeout=30,
        idle_seconds=0,
    )

    inserted = await worker.run_once()

    after_duplicates = duplicate_update_total.labels(Platform.MAX.value)._value.get()
    assert inserted == 2
    assert state.cursor == 33_113_684
    assert len(state.inserted_updates) == 2
    assert state.inserted_updates[0]["update_type"] == "message_created"
    assert state.inserted_updates[1]["update_type"] == "message_edited"
    assert (
        state.inserted_updates[0]["update_key"]
        != state.inserted_updates[1]["update_key"]
    )
    assert after_duplicates == before_duplicates
