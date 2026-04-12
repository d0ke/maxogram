from __future__ import annotations

from sqlalchemy.exc import OperationalError

from maxogram.platforms.base import PlatformDeliveryError
from maxogram.runtime_resilience import (
    RuntimeBackoffState,
    is_retryable_worker_error,
    is_transient_db_error,
    runtime_backoff_delay_seconds,
)


def test_runtime_backoff_delay_caps_at_thirty_seconds():
    delays = [runtime_backoff_delay_seconds(attempt) for attempt in range(1, 8)]

    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_runtime_backoff_state_resets_after_success():
    state = RuntimeBackoffState()

    assert state.next_delay_seconds() == 1.0
    assert state.next_delay_seconds() == 2.0
    assert state.clear() == 2
    assert state.attempts == 0


def test_transient_db_error_detects_wrapped_os_error():
    exc = OperationalError("SELECT 1", {}, OSError("vpn down"))

    assert is_transient_db_error(exc) is True
    assert is_retryable_worker_error(exc) is True


def test_non_transient_db_error_is_not_retryable():
    exc = OperationalError("SELECT 1", {}, RuntimeError("bad credentials"))

    assert is_transient_db_error(exc) is False


def test_retryable_worker_error_respects_platform_flag():
    retryable = PlatformDeliveryError("temporary", retryable=True)
    permanent = PlatformDeliveryError("forbidden", retryable=False)

    assert is_retryable_worker_error(retryable) is True
    assert is_retryable_worker_error(permanent) is False
