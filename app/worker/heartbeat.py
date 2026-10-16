import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from app.core.metrics import HEARTBEAT_LAG_SECONDS
from app.queue.base import QueueBackend
from app.worker.context import JobContext

logger = logging.getLogger(__name__)


class HeartbeatTask:
    """
    Independent background task that periodically sends heartbeats
    for a running job (ADR-4).
    If heartbeat fails (lease lost or fencing token stale), marks the context invalid
    and triggers cancellation on the handler.
    """

    def __init__(
        self,
        queue: QueueBackend,
        job_id: uuid.UUID,
        fencing_token: int,
        lease_ttl: int = 30,
        interval_seconds: float | None = None,
        context: JobContext | None = None,
        on_lease_lost: Callable[[], None] | None = None,
        job_type: str = "default",
    ) -> None:
        self.queue = queue
        self.job_id = job_id
        self.fencing_token = fencing_token
        self.lease_ttl = lease_ttl
        # By default, heartbeat every TTL / 3
        self.interval_seconds = interval_seconds or max(0.1, lease_ttl / 3.0)
        self.context = context
        self.on_lease_lost = on_lease_lost
        self.job_type = job_type
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._lease_deadline = time.monotonic() + lease_ttl

    async def _loop(self) -> None:
        while self._running:
            target_time = time.monotonic() + self.interval_seconds
            try:
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break

            if not self._running:
                break

            lag = max(0.0, time.monotonic() - target_time)
            HEARTBEAT_LAG_SECONDS.observe(lag)

            try:
                if self.job_type and self.job_type != "default":
                    extended = await self.queue.heartbeat(
                        self.job_id,
                        self.fencing_token,
                        lease_ttl=self.lease_ttl,
                        job_type=self.job_type,
                    )
                else:
                    extended = await self.queue.heartbeat(
                        self.job_id,
                        self.fencing_token,
                        lease_ttl=self.lease_ttl,
                    )
                if not extended:
                    logger.warning(
                        "Heartbeat rejected for job '%s' (token=%d): lease lost or stale",
                        self.job_id,
                        self.fencing_token,
                    )
                    self._handle_lease_lost()
                    break

                self._lease_deadline = time.monotonic() + self.lease_ttl
                logger.debug(
                    "Heartbeat succeeded for job '%s' (token=%d, ttl=%ds)",
                    self.job_id,
                    self.fencing_token,
                    self.lease_ttl,
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error sending heartbeat for job '%s': %s", self.job_id, e)
                if time.monotonic() >= self._lease_deadline:
                    logger.warning(
                        "Local lease deadline exceeded for job '%s'; cancelling handler",
                        self.job_id,
                    )
                    self._handle_lease_lost()
                    break

    def _handle_lease_lost(self) -> None:
        self._running = False
        if self.context:
            self.context.invalidate_lease()
        if self.on_lease_lost:
            try:
                self.on_lease_lost()
            except Exception as e:
                logger.error("Error executing on_lease_lost callback: %s", e)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(
                self._loop(),
                name=f"heartbeat-{self.job_id}-{self.fencing_token}",
            )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def __aenter__(self) -> "HeartbeatTask":
        self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
