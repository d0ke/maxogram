from __future__ import annotations

from maxogram.domain import OutboxAction
from maxogram.services.retry import max_attempts_for, retry_decision


def test_max_attempts_by_action():
    assert max_attempts_for(OutboxAction.SEND) == 10
    assert max_attempts_for(OutboxAction.EDIT) == 6
    assert max_attempts_for(OutboxAction.DELETE) == 6


def test_retry_decision_stops_on_permanent_error():
    decision = retry_decision(OutboxAction.SEND, 1, retryable_error=False)

    assert decision.retryable is False
    assert decision.next_attempt_at is None


def test_retry_decision_schedules_retry_before_limit():
    decision = retry_decision(OutboxAction.SEND, 1, retryable_error=True)

    assert decision.retryable is True
    assert decision.next_attempt_at is not None
