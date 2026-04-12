from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass

from sqlalchemy.exc import DBAPIError

from maxogram.platforms.base import PlatformDeliveryError

try:
    from asyncpg import exceptions as asyncpg_exceptions
except ImportError:  # pragma: no cover
    asyncpg_exceptions = None

_TRANSIENT_ASYNCPG_EXCEPTIONS = tuple(
    exc_type
    for exc_type in (
        getattr(asyncpg_exceptions, "CannotConnectNowError", None),
        getattr(asyncpg_exceptions, "ConnectionDoesNotExistError", None),
        getattr(asyncpg_exceptions, "ConnectionFailureError", None),
        getattr(asyncpg_exceptions, "PostgresConnectionError", None),
        getattr(asyncpg_exceptions, "TooManyConnectionsError", None),
    )
    if exc_type is not None
)


@dataclass(slots=True)
class RuntimeBackoffState:
    attempts: int = 0

    def next_delay_seconds(self) -> float:
        self.attempts += 1
        return runtime_backoff_delay_seconds(self.attempts)

    def clear(self) -> int:
        previous_attempts = self.attempts
        self.attempts = 0
        return previous_attempts


def runtime_backoff_delay_seconds(
    attempt: int,
    *,
    base_seconds: float = 1.0,
    max_seconds: float = 30.0,
) -> float:
    normalized_attempt = max(attempt - 1, 0)
    return float(min(base_seconds * (2**normalized_attempt), max_seconds))


async def wait_or_stop(stop_event: asyncio.Event, delay_seconds: float) -> bool:
    if stop_event.is_set():
        return True
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
    except TimeoutError:
        return False
    return True


def is_retryable_worker_error(exc: BaseException) -> bool:
    return is_transient_db_error(exc) or (
        isinstance(exc, PlatformDeliveryError) and exc.retryable
    )


def is_transient_db_error(exc: BaseException) -> bool:
    for candidate in _iter_exception_chain(exc):
        if isinstance(candidate, (OSError, TimeoutError)):
            return True
        if isinstance(candidate, _TRANSIENT_ASYNCPG_EXCEPTIONS):
            return True
        if isinstance(candidate, DBAPIError) and candidate.connection_invalidated:
            return True
    return False


def _iter_exception_chain(exc: BaseException) -> list[BaseException]:
    queue: deque[BaseException] = deque([exc])
    seen: set[int] = set()
    chain: list[BaseException] = []
    while queue:
        current = queue.popleft()
        current_id = id(current)
        if current_id in seen:
            continue
        seen.add(current_id)
        chain.append(current)
        for nested in (
            getattr(current, "orig", None),
            current.__cause__,
            current.__context__,
        ):
            if isinstance(nested, BaseException):
                queue.append(nested)
    return chain
