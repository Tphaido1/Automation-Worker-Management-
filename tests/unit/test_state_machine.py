import pytest
from hypothesis import given
from hypothesis import strategies as st

from app.domain.errors import InvalidStateTransitionError
from app.domain.state_machine import (
    TERMINAL_STATES,
    VALID_TRANSITIONS,
    JobStatus,
    can_cancel,
    is_terminal,
    transition,
)

ALL_STATUSES = list(JobStatus)


def test_exhaustive_all_pairs_matrix() -> None:
    """
    Exhaustively tests all 49 (7x7) possible state transitions.
    Verifies that only authorized transitions succeed, and all others raise InvalidStateTransitionError.
    """
    valid_count = 0
    invalid_count = 0

    for current in ALL_STATUSES:
        for target in ALL_STATUSES:
            if target in VALID_TRANSITIONS[current]:
                result = transition(current, target)
                assert result == target
                valid_count += 1
            else:
                with pytest.raises(InvalidStateTransitionError) as exc_info:
                    transition(current, target)
                assert exc_info.value.from_state == current.value
                assert exc_info.value.to_state == target.value
                invalid_count += 1

    # Exactly 11 valid transitions according to specification 3.1
    assert valid_count == 11
    # Exactly 38 invalid transitions
    assert invalid_count == 38
    assert valid_count + invalid_count == 49


@pytest.mark.parametrize(
    ("from_status", "to_status"),
    [
        (JobStatus.PENDING, JobStatus.QUEUED),
        (JobStatus.PENDING, JobStatus.CANCELLED),
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.RETRY_WAITING),
        (JobStatus.RUNNING, JobStatus.DEAD_LETTER),
        (JobStatus.RUNNING, JobStatus.CANCELLED),
        (JobStatus.RETRY_WAITING, JobStatus.QUEUED),
        (JobStatus.RETRY_WAITING, JobStatus.CANCELLED),
        (JobStatus.DEAD_LETTER, JobStatus.QUEUED),
    ],
)
def test_valid_transitions_succeed(from_status: JobStatus, to_status: JobStatus) -> None:
    """Verifies each authorized transition in isolation."""
    assert transition(from_status, to_status) == to_status


@pytest.mark.parametrize("terminal_status", list(TERMINAL_STATES))
def test_terminal_states_never_transition(terminal_status: JobStatus) -> None:
    """Invariant 7: Terminal states (SUCCEEDED, CANCELLED) cannot transition to any state."""
    assert is_terminal(terminal_status) is True
    assert can_cancel(terminal_status) is False

    for target in ALL_STATUSES:
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            transition(terminal_status, target)
        assert "already in terminal state" in exc_info.value.reason


def test_can_cancel_rules() -> None:
    """Verifies which states allow cancellation."""
    assert can_cancel(JobStatus.PENDING) is True
    assert can_cancel(JobStatus.QUEUED) is True
    assert can_cancel(JobStatus.RUNNING) is True
    assert can_cancel(JobStatus.RETRY_WAITING) is True

    # Terminal and dead letter states
    assert can_cancel(JobStatus.SUCCEEDED) is False
    assert can_cancel(JobStatus.CANCELLED) is False
    assert can_cancel(JobStatus.DEAD_LETTER) is False


# Property-Based Testing with Hypothesis
@given(st.sampled_from(ALL_STATUSES), st.sampled_from(ALL_STATUSES))
def test_property_transition_safety(current: JobStatus, target: JobStatus) -> None:
    """Property test: No transition ever violates the VALID_TRANSITIONS contract."""
    if target in VALID_TRANSITIONS[current]:
        assert transition(current, target) == target
    else:
        with pytest.raises(InvalidStateTransitionError):
            transition(current, target)
