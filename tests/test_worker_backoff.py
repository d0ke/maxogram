from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.exc import OperationalError

import maxogram.workers.delivery as delivery_module
import maxogram.workers.normalizer as normalizer_module
import maxogram.workers.pollers as pollers_module
import maxogram.workers.reconciliation as reconciliation_module
from maxogram.domain import Platform
from maxogram.platforms.base import PlatformDeliveryError
from maxogram.workers.delivery import DeliveryWorker
from maxogram.workers.normalizer import NormalizerWorker
from maxogram.workers.pollers import PollerWorker
from maxogram.workers.reconciliation import ReconciliationWorker


def make_transient_db_error() -> OperationalError:
    return OperationalError("SELECT 1", {}, OSError("vpn down"))


async def record_wait(
    delays: list[float],
    stop_event: asyncio.Event,
    delay_seconds: float,
) -> bool:
    if stop_event.is_set():
        return True
    delays.append(delay_seconds)
    return False


@pytest.mark.asyncio
async def test_poller_worker_uses_exponential_backoff_for_retryable_platform_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = PollerWorker(
        database=cast(Any, None),
        platform=Platform.TELEGRAM,
        bot_id=uuid.uuid4(),
        client=cast(Any, object()),
        stop_event=asyncio.Event(),
        limit=100,
        timeout=30,
        idle_seconds=0.25,
    )
    delays: list[float] = []
    calls = {"count": 0}

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        return await record_wait(delays, stop_event, delay_seconds)

    async def fake_run_once() -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            raise PlatformDeliveryError("temporary", retryable=True)
        worker.stop_event.set()
        return 1

    monkeypatch.setattr(pollers_module, "wait_or_stop", fake_wait_or_stop)
    worker.run_once = fake_run_once  # type: ignore[method-assign]
    caplog.set_level("INFO")

    await worker.run()

    assert delays == [1.0]
    assert "telegram-poller recovered after 1 temporary failure(s)" in caplog.text


@pytest.mark.asyncio
async def test_poller_worker_uses_exponential_backoff_for_transient_db_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = PollerWorker(
        database=cast(Any, None),
        platform=Platform.MAX,
        bot_id=uuid.uuid4(),
        client=cast(Any, object()),
        stop_event=asyncio.Event(),
        limit=100,
        timeout=30,
        idle_seconds=0.25,
    )
    delays: list[float] = []
    calls = {"count": 0}

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        return await record_wait(delays, stop_event, delay_seconds)

    async def fake_run_once() -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            raise make_transient_db_error()
        worker.stop_event.set()
        return 1

    monkeypatch.setattr(pollers_module, "wait_or_stop", fake_wait_or_stop)
    worker.run_once = fake_run_once  # type: ignore[method-assign]

    await worker.run()

    assert delays == [1.0]


@pytest.mark.asyncio
async def test_normalizer_worker_uses_backoff_only_for_transient_db_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = NormalizerWorker(
        database=cast(Any, None),
        clients={},
        command_processor=cast(Any, None),
        stop_event=asyncio.Event(),
        idle_seconds=0.25,
    )
    delays: list[float] = []
    calls = {"count": 0}

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        return await record_wait(delays, stop_event, delay_seconds)

    async def fake_run_once() -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            raise PlatformDeliveryError("temporary", retryable=True)
        worker.stop_event.set()
        return 1

    monkeypatch.setattr(normalizer_module, "wait_or_stop", fake_wait_or_stop)
    worker.run_once = fake_run_once  # type: ignore[method-assign]

    await worker.run()

    assert delays == [0.25]


@pytest.mark.asyncio
async def test_reconciliation_worker_uses_backoff_for_transient_db_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = ReconciliationWorker(
        database=cast(Any, None),
        stop_event=asyncio.Event(),
        idle_seconds=0.25,
    )
    delays: list[float] = []
    calls = {"count": 0}

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        return await record_wait(delays, stop_event, delay_seconds)

    async def fake_run_once() -> tuple[int, int, int]:
        calls["count"] += 1
        if calls["count"] == 1:
            raise make_transient_db_error()
        worker.stop_event.set()
        return 0, 0, 0

    monkeypatch.setattr(reconciliation_module, "wait_or_stop", fake_wait_or_stop)
    worker.run_once = fake_run_once  # type: ignore[method-assign]

    await worker.run()

    assert delays == [1.0]


@pytest.mark.asyncio
async def test_delivery_worker_uses_backoff_for_transient_db_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = DeliveryWorker(
        database=cast(Any, None),
        clients={},
        stop_event=asyncio.Event(),
        lease_seconds=60,
        idle_seconds=0.25,
        root_dir=Path.cwd(),
    )
    delays: list[float] = []
    calls = {"count": 0}

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        return await record_wait(delays, stop_event, delay_seconds)

    async def fake_run_once() -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            raise make_transient_db_error()
        worker.stop_event.set()
        return 1

    monkeypatch.setattr(delivery_module, "wait_or_stop", fake_wait_or_stop)
    worker.run_once = fake_run_once  # type: ignore[method-assign]

    await worker.run()

    assert delays == [1.0]
