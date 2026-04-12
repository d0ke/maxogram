from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from maxogram.domain import OutboxAction


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retryable: bool
    max_attempts: int
    next_attempt_at: datetime | None = None


def compute_backoff(
    attempt: int,
    *,
    base_seconds: float = 2.0,
    max_seconds: float = 900.0,
    jitter_ratio: float = 0.2,
) -> timedelta:
    delay = min(base_seconds * (2 ** max(attempt - 1, 0)), max_seconds)
    jitter = random.uniform(0, delay * jitter_ratio)
    return timedelta(seconds=delay + jitter)


def max_attempts_for(action: OutboxAction) -> int:
    return 10 if action == OutboxAction.SEND else 6


def retry_decision(
    action: OutboxAction,
    attempt: int,
    *,
    retryable_error: bool,
) -> RetryDecision:
    max_attempts = max_attempts_for(action)
    if not retryable_error or attempt >= max_attempts:
        return RetryDecision(retryable=False, max_attempts=max_attempts)
    return RetryDecision(
        retryable=True,
        max_attempts=max_attempts,
        next_attempt_at=datetime.now(UTC) + compute_backoff(attempt),
    )
