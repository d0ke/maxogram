from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

import maxogram.app as app_module
from maxogram.app import MaxogramApp
from maxogram.config import AppSettings, DatabaseConfig
from maxogram.domain import Platform


@dataclass
class BootstrapState:
    failures: list[BaseException] = field(default_factory=list)
    session_attempts: int = 0
    commits: int = 0
    proxy_profiles: list[Platform] = field(default_factory=list)
    bot_ids: dict[Platform, uuid.UUID] = field(
        default_factory=lambda: {
            Platform.TELEGRAM: uuid.uuid4(),
            Platform.MAX: uuid.uuid4(),
        }
    )


class FakeSession:
    def __init__(self, state: BootstrapState, attempt_no: int) -> None:
        self.state = state
        self.attempt_no = attempt_no

    async def commit(self) -> None:
        self.state.commits += 1


class FakeDatabase:
    def __init__(self, state: BootstrapState) -> None:
        self.state = state

    @asynccontextmanager
    async def session(self):
        self.state.session_attempts += 1
        yield FakeSession(self.state, self.state.session_attempts)


class FakeRepository:
    def __init__(self, session: FakeSession) -> None:
        self.session = session
        self.state = session.state

    async def ensure_bot_credential(self, platform: Platform) -> uuid.UUID:
        if (
            platform == Platform.TELEGRAM
            and self.session.attempt_no <= len(self.state.failures)
        ):
            raise self.state.failures[self.session.attempt_no - 1]
        return self.state.bot_ids[platform]

    async def ensure_proxy_profile(self, platform: Platform) -> None:
        self.state.proxy_profiles.append(platform)


def make_settings() -> AppSettings:
    return AppSettings(
        telegram_token="telegram-token",
        max_token="max-token",
        db=DatabaseConfig(
            database="maxogram",
            user="maxogram_app",
            password="secret",
            host="localhost",
            port=5432,
        ),
        vps_host=None,
        vps_ssh_port=None,
        root_dir=Path.cwd(),
    )


def make_app(monkeypatch: pytest.MonkeyPatch, state: BootstrapState) -> MaxogramApp:
    monkeypatch.setattr(app_module, "Repository", FakeRepository)
    app = MaxogramApp(make_settings())
    app.database = FakeDatabase(state)  # type: ignore[assignment]
    return app


@pytest.mark.asyncio
async def test_bootstrap_database_succeeds_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = BootstrapState()
    app = make_app(monkeypatch, state)
    delays: list[float] = []

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        if stop_event.is_set():
            return True
        delays.append(delay_seconds)
        return False

    monkeypatch.setattr(app_module, "wait_or_stop", fake_wait_or_stop)

    telegram_bot_id, max_bot_id = await app._bootstrap_database_until_ready()

    assert telegram_bot_id == state.bot_ids[Platform.TELEGRAM]
    assert max_bot_id == state.bot_ids[Platform.MAX]
    assert state.commits == 1
    assert state.proxy_profiles == [Platform.TELEGRAM, Platform.MAX]
    assert delays == []


@pytest.mark.asyncio
async def test_bootstrap_database_retries_transient_error_then_recovers(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state = BootstrapState(
        failures=[OperationalError("SELECT 1", {}, OSError("vpn down"))]
    )
    app = make_app(monkeypatch, state)
    delays: list[float] = []

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        if stop_event.is_set():
            return True
        delays.append(delay_seconds)
        return False

    monkeypatch.setattr(app_module, "wait_or_stop", fake_wait_or_stop)
    caplog.set_level("INFO")

    telegram_bot_id, max_bot_id = await app._bootstrap_database_until_ready()

    assert telegram_bot_id == state.bot_ids[Platform.TELEGRAM]
    assert max_bot_id == state.bot_ids[Platform.MAX]
    assert delays == [1.0]
    assert state.session_attempts == 2
    assert "Database bootstrap recovered after 1 temporary failure(s)" in caplog.text


@pytest.mark.asyncio
async def test_bootstrap_database_uses_capped_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = BootstrapState(
        failures=[
            OperationalError("SELECT 1", {}, OSError("vpn down"))
            for _ in range(7)
        ]
    )
    app = make_app(monkeypatch, state)
    delays: list[float] = []

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        if stop_event.is_set():
            return True
        delays.append(delay_seconds)
        return False

    monkeypatch.setattr(app_module, "wait_or_stop", fake_wait_or_stop)

    await app._bootstrap_database_until_ready()

    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


@pytest.mark.asyncio
async def test_bootstrap_database_does_not_retry_non_transient_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = BootstrapState(
        failures=[OperationalError("SELECT 1", {}, RuntimeError("bad credentials"))]
    )
    app = make_app(monkeypatch, state)
    delays: list[float] = []

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        if stop_event.is_set():
            return True
        delays.append(delay_seconds)
        return False

    monkeypatch.setattr(app_module, "wait_or_stop", fake_wait_or_stop)

    with pytest.raises(OperationalError):
        await app._bootstrap_database_until_ready()

    assert delays == []
    assert state.session_attempts == 1


@pytest.mark.asyncio
async def test_bootstrap_database_stops_during_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = BootstrapState(
        failures=[OperationalError("SELECT 1", {}, OSError("vpn down"))]
    )
    app = make_app(monkeypatch, state)

    async def fake_wait_or_stop(stop_event, delay_seconds: float) -> bool:
        _ = delay_seconds
        stop_event.set()
        return True

    monkeypatch.setattr(app_module, "wait_or_stop", fake_wait_or_stop)

    bootstrap = await app._bootstrap_database_until_ready()

    assert bootstrap is None
    assert state.session_attempts == 1
