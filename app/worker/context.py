import uuid
from dataclasses import dataclass
from typing import Any


@dataclass
class JobContext:
    """Execution context passed to job handlers (ADR-5)."""

    job_id: uuid.UUID
    job_type: str
    attempt: int
    fencing_token: int
    payload: dict[str, Any]
    idempotency_key: str | None = None
    timeout_seconds: int = 60
    _lease_valid: bool = True

    @property
    def is_lease_valid(self) -> bool:
        """Returns False if heartbeat detects lease was lost."""
        return self._lease_valid

    def invalidate_lease(self) -> None:
        self._lease_valid = False
