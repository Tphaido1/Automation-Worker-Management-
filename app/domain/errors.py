class LeaseFlowError(Exception):
    """Base exception for all LeaseFlow domain errors."""

    pass


class InvalidStateTransitionError(LeaseFlowError):
    """Raised when an illegal job state transition is attempted."""

    def __init__(self, from_state: str, to_state: str, reason: str = "") -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.reason = reason
        message = f"Invalid state transition: '{from_state}' -> '{to_state}'"
        if reason:
            message += f" ({reason})"
        super().__init__(message)


class JobNotFoundError(LeaseFlowError):
    """Raised when a requested job does not exist."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Job with ID '{job_id}' not found")


class JobAlreadyTerminalError(LeaseFlowError):
    """Raised when attempting an operation on a job that is already in a terminal state."""

    def __init__(self, job_id: str, status: str) -> None:
        self.job_id = job_id
        self.status = status
        super().__init__(
            f"Job '{job_id}' is already in terminal state '{status}' and cannot be altered"
        )
