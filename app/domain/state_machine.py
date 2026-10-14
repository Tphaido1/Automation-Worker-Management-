from enum import StrEnum

from app.domain.errors import InvalidStateTransitionError


class JobStatus(StrEnum):
    """Lifecycle state of a job in LeaseFlow (ADR-6)."""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    RETRY_WAITING = "RETRY_WAITING"
    DEAD_LETTER = "DEAD_LETTER"
    CANCELLED = "CANCELLED"


class ExecutionOutcome(StrEnum):
    """Result of a specific execution attempt recorded in executions table."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    TIMED_OUT = "TIMED_OUT"
    REJECTED_STALE = "REJECTED_STALE"
    CANCELLED = "CANCELLED"


# Invariant 7: Terminal states are truly terminal (no further transitions)
TERMINAL_STATES: frozenset[JobStatus] = frozenset(
    {
        JobStatus.SUCCEEDED,
        JobStatus.CANCELLED,
    }
)

# Single authoritative state transition table (section 3.1)
VALID_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.RETRY_WAITING,
            JobStatus.DEAD_LETTER,
            JobStatus.CANCELLED,
        }
    ),
    JobStatus.RETRY_WAITING: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    JobStatus.DEAD_LETTER: frozenset({JobStatus.QUEUED}),  # Manual operator retry only
    JobStatus.SUCCEEDED: frozenset(),  # Terminal
    JobStatus.CANCELLED: frozenset(),  # Terminal
}


def transition(current_status: JobStatus, target_status: JobStatus) -> JobStatus:
    """
    Validates and executes a state transition.
    Raises InvalidStateTransitionError if the transition is illegal.
    """
    valid_targets = VALID_TRANSITIONS.get(current_status, frozenset())
    if target_status not in valid_targets:
        if is_terminal(current_status):
            reason = f"Job is already in terminal state '{current_status.value}'"
        else:
            reason = f"Allowed transitions from '{current_status.value}' are: {[s.value for s in valid_targets]}"
        raise InvalidStateTransitionError(current_status.value, target_status.value, reason=reason)

    return target_status


def is_terminal(status: JobStatus) -> bool:
    """Returns True if the status is an immutable terminal state."""
    return status in TERMINAL_STATES


def can_cancel(status: JobStatus) -> bool:
    """Returns True if the job can be cancelled by a client."""
    return JobStatus.CANCELLED in VALID_TRANSITIONS.get(status, frozenset())
