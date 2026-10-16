import asyncio
import uuid
from dataclasses import dataclass, field
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
    pending_outbox_records: list[dict[str, Any]] = field(default_factory=list)

    @property
    def is_lease_valid(self) -> bool:
        """Returns False if heartbeat detects lease was lost."""
        return self._lease_valid

    def invalidate_lease(self) -> None:
        self._lease_valid = False

    def emit_outbox(
        self,
        kind: str,
        payload: dict[str, Any],
        dedup_key: str | None = None,
    ) -> None:
        """
        Buffers an outbox event to be atomically committed with job completion (ADR-5).
        If dedup_key is omitted, defaults to '{idempotency_key or job_id}:{kind}'.
        """
        if dedup_key is None:
            base_key = self.idempotency_key or str(self.job_id)
            dedup_key = f"{base_key}:{kind}"

        self.pending_outbox_records.append(
            {
                "job_id": self.job_id,
                "kind": kind,
                "payload": payload,
                "dedup_key": dedup_key,
            }
        )

    async def sleep(self, seconds: float, check_interval: float = 0.1) -> None:
        """
        Cooperative sleep that continuously checks lease validity.
        Raises asyncio.CancelledError immediately if lease is lost.
        """
        elapsed = 0.0
        while elapsed < seconds:
            if not self._lease_valid:
                raise asyncio.CancelledError("Lease invalidated during handler sleep")
            step = min(check_interval, seconds - elapsed)
            await asyncio.sleep(step)
            elapsed += step
