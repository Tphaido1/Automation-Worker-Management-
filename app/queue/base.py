import uuid
from abc import ABC, abstractmethod
from typing import Any

from app.db.models import JobModel


class QueueBackend(ABC):
    """Abstract interface for job claim, lease allocation, and queue operations (ADR-1)."""

    @abstractmethod
    async def claim(self, worker_id: str, lease_ttl: int = 30) -> JobModel | None:
        """
        Atomically claims the highest priority ready job.
        Increments fencing_token, updates lease_owner and lease_expires_at,
        and creates an execution record in the same transaction.
        Returns the claimed JobModel or None if no jobs are available.
        """
        ...

    @abstractmethod
    async def complete(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        result: dict[str, Any] | None = None,
        outbox_records: list[dict[str, Any]] | None = None,
        job_type: str = "default",
    ) -> bool:
        """
        Atomically completes the job with SUCCEEDED status and writes outbox events (ADR-5).
        Requires matching fencing_token (ADR-3).
        Returns True if committed successfully, False if token is stale (REJECTED_STALE).
        """
        ...

    @abstractmethod
    async def fail(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        error: str,
        retryable: bool = True,
        backoff_seconds: float = 0.0,
        job_type: str = "default",
    ) -> bool:
        """
        Atomically records a failure for the job attempt.
        Fenced by fencing_token (ADR-3).
        If retryable and attempt < max_retries, transitions to RETRY_WAITING with run_at = now + backoff_seconds.
        Otherwise, transitions to DEAD_LETTER (ADR-5).
        Returns True if updated successfully, False if token is stale (REJECTED_STALE).
        """
        ...

    @abstractmethod
    async def heartbeat(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        lease_ttl: int = 30,
        job_type: str = "default",
    ) -> bool:
        """
        Atomically extends lease_expires_at for a RUNNING job.
        Requires matching fencing_token (ADR-3, ADR-4).
        Returns True if lease extended, False if token is stale or job is no longer RUNNING.
        """
        ...
