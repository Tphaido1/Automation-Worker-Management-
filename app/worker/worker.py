import asyncio
import inspect
import logging
import uuid
from typing import Any

from app.core.logging import (
    ctx_attempt,
    ctx_fencing_token,
    ctx_job_id,
    ctx_worker_id,
)
from app.queue.base import QueueBackend
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry

logger = logging.getLogger(__name__)


class Worker:
    """
    Distributed worker instance that claims jobs via QueueBackend
    and dispatches them to registered handlers.
    """

    def __init__(
        self,
        worker_id: str | None = None,
        queue: QueueBackend | None = None,
        registry: HandlerRegistry | None = None,
        lease_ttl: int = 30,
        concurrency: int = 1,
        poll_interval: float = 0.2,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queue = queue
        self.registry = registry or HandlerRegistry()
        self.lease_ttl = lease_ttl
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self._running = False
        self._semaphore = asyncio.Semaphore(concurrency)
        self._tasks: set[asyncio.Task[Any]] = set()

    async def run_once(self) -> bool:
        """
        Attempts to claim and execute one job.
        Returns True if a job was found and processed, False if queue was empty.
        """
        if not self.queue:
            raise RuntimeError("QueueBackend is required to run worker")

        job = await self.queue.claim(self.worker_id, lease_ttl=self.lease_ttl)
        if not job:
            return False

        # Set distributed tracing context variables
        t_job = ctx_job_id.set(str(job.id))
        t_worker = ctx_worker_id.set(self.worker_id)
        t_token = ctx_fencing_token.set(job.fencing_token)
        t_att = ctx_attempt.set(job.attempt)

        try:
            handler = self.registry.get_handler(job.type)
            if not handler:
                err_msg = f"No registered handler for job type '{job.type}'"
                logger.error(err_msg)
                await self.queue.fail(job.id, job.fencing_token, error=err_msg)
                return True

            context = JobContext(
                job_id=job.id,
                job_type=job.type,
                attempt=job.attempt,
                fencing_token=job.fencing_token,
                payload=job.payload,
                idempotency_key=job.idempotency_key,
                timeout_seconds=job.timeout_seconds,
            )

            # Execute handler safely: async vs sync
            result: Any = None
            try:
                if inspect.iscoroutinefunction(handler):
                    result = await handler(context)
                else:
                    result = await asyncio.to_thread(handler, context)

                # Format result into dictionary if possible
                result_payload = result if isinstance(result, dict) else {"output": result}
                await self.queue.complete(job.id, job.fencing_token, result=result_payload)
                logger.info("Successfully executed job '%s' (token=%d)", job.id, job.fencing_token)
            except Exception as e:
                logger.error("Job '%s' failed with error: %s", job.id, e, exc_info=True)
                await self.queue.fail(job.id, job.fencing_token, error=str(e))

            return True
        finally:
            ctx_job_id.reset(t_job)
            ctx_worker_id.reset(t_worker)
            ctx_fencing_token.reset(t_token)
            ctx_attempt.reset(t_att)

    async def _worker_loop(self) -> None:
        """Main background loop polling for jobs."""
        while self._running:
            try:
                async with self._semaphore:
                    claimed = await self.run_once()
                    if not claimed:
                        await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error("Unhandled error in worker loop: %s", e, exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def start(self) -> None:
        """Starts worker loop in background."""
        if not self._running:
            self._running = True
            for i in range(self.concurrency):
                task = asyncio.create_task(self._worker_loop(), name=f"{self.worker_id}-loop-{i}")
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def stop(self) -> None:
        """Gracefully stops worker."""
        self._running = False
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
