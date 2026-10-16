import asyncio
import uuid
from typing import Any

from app.db.models import JobModel
from app.queue.base import QueueBackend


class FaultInjectionBackend(QueueBackend):
    """
    Transparent decorator over any QueueBackend that injects synthetic faults
    such as network latency, connection drops, and query timeouts.
    """

    def __init__(self, backend: QueueBackend) -> None:
        self.backend = backend
        self._latencies: dict[str, float] = {}
        self._failures: dict[str, list[Exception]] = {}
        self._drop_network = False

    def set_latency(self, method: str, delay_seconds: float) -> None:
        """Injects artificial latency before invoking the specified backend method."""
        self._latencies[method] = delay_seconds

    def inject_failure(self, method: str, exc: Exception) -> None:
        """Injects a one-off exception on the next invocation of the specified method."""
        if method not in self._failures:
            self._failures[method] = []
        self._failures[method].append(exc)

    def set_network_partition(self, partition: bool) -> None:
        """Simulates complete network partition (all calls raise ConnectionError)."""
        self._drop_network = partition

    def reset(self) -> None:
        """Clears all configured faults."""
        self._latencies.clear()
        self._failures.clear()
        self._drop_network = False

    async def _before_call(self, method: str) -> None:
        if self._drop_network:
            raise ConnectionError(f"Simulated network partition during {method}()")

        if method in self._latencies:
            await asyncio.sleep(self._latencies[method])

        if method in self._failures and self._failures[method]:
            exc = self._failures[method].pop(0)
            raise exc

    async def claim(self, worker_id: str, lease_ttl: int = 30) -> JobModel | None:
        await self._before_call("claim")
        return await self.backend.claim(worker_id, lease_ttl=lease_ttl)

    async def complete(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        result: dict[str, Any] | None = None,
        outbox_records: list[dict[str, Any]] | None = None,
        job_type: str = "default",
    ) -> bool:
        await self._before_call("complete")
        return await self.backend.complete(
            job_id,
            fencing_token,
            result=result,
            outbox_records=outbox_records,
            job_type=job_type,
        )

    async def fail(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        error: str,
        retryable: bool = True,
        backoff_seconds: float = 0.0,
        job_type: str = "default",
    ) -> bool:
        await self._before_call("fail")
        return await self.backend.fail(
            job_id,
            fencing_token,
            error=error,
            retryable=retryable,
            backoff_seconds=backoff_seconds,
            job_type=job_type,
        )

    async def heartbeat(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        lease_ttl: int = 30,
        job_type: str = "default",
    ) -> bool:
        await self._before_call("heartbeat")
        return await self.backend.heartbeat(
            job_id,
            fencing_token,
            lease_ttl=lease_ttl,
            job_type=job_type,
        )
