import asyncio
import inspect
import logging
import time
import uuid
from typing import Any

from app.core.logging import (
    ctx_attempt,
    ctx_fencing_token,
    ctx_job_id,
    ctx_worker_id,
)
from app.core.metrics import JOB_DURATION_SECONDS, WORKERS_ACTIVE
from app.domain.retry_policy import RetryPolicy, is_retryable
from app.queue.base import QueueBackend
from app.worker.context import JobContext
from app.worker.heartbeat import HeartbeatTask
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
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queue = queue
        self.registry = registry or HandlerRegistry()
        self.lease_ttl = lease_ttl
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.retry_policy = retry_policy or RetryPolicy()
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
                await self.queue.fail(
                    job.id,
                    job.fencing_token,
                    error=err_msg,
                    retryable=False,
                    backoff_seconds=0.0,
                    job_type=job.type,
                )
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

            handler_task: asyncio.Task[Any] | None = None

            def on_lease_lost() -> None:
                if handler_task and not handler_task.done():
                    logger.warning("Cancelling job '%s' handler because lease was lost", job.id)
                    handler_task.cancel()

            heartbeat = HeartbeatTask(
                queue=self.queue,
                job_id=job.id,
                fencing_token=job.fencing_token,
                lease_ttl=self.lease_ttl,
                context=context,
                on_lease_lost=on_lease_lost,
                job_type=job.type,
            )

            # Execute handler safely: async vs sync with active heartbeat
            result: Any = None
            exec_start = time.perf_counter()
            try:
                async with heartbeat:
                    if inspect.iscoroutinefunction(handler):
                        handler_task = asyncio.create_task(handler(context))
                    else:
                        handler_task = asyncio.create_task(asyncio.to_thread(handler, context))

                    result = await handler_task

                duration = time.perf_counter() - exec_start
                JOB_DURATION_SECONDS.labels(type=job.type, outcome="succeeded").observe(duration)

                if not context.is_lease_valid:
                    logger.warning(
                        "Job '%s' finished but lease is invalid; skipping commit",
                        job.id,
                    )
                    return True

                # Format result into dictionary if possible
                result_payload = result if isinstance(result, dict) else {"output": result}
                await self.queue.complete(
                    job.id,
                    job.fencing_token,
                    result=result_payload,
                    outbox_records=context.pending_outbox_records
                    if context.pending_outbox_records
                    else None,
                    job_type=job.type,
                )
                logger.info("Successfully executed job '%s' (token=%d)", job.id, job.fencing_token)
            except asyncio.CancelledError:
                duration = time.perf_counter() - exec_start
                JOB_DURATION_SECONDS.labels(type=job.type, outcome="cancelled").observe(duration)
                logger.warning(
                    "Job '%s' execution cancelled (lease lost or worker shutdown)", job.id
                )
                if context.is_lease_valid:
                    delay = self.retry_policy.compute_delay(job.attempt)
                    await self.queue.fail(
                        job.id,
                        job.fencing_token,
                        error="Execution cancelled",
                        retryable=True,
                        backoff_seconds=delay,
                        job_type=job.type,
                    )
            except Exception as e:
                duration = time.perf_counter() - exec_start
                JOB_DURATION_SECONDS.labels(type=job.type, outcome="failed").observe(duration)
                logger.error("Job '%s' failed with error: %s", job.id, e, exc_info=True)
                if context.is_lease_valid:
                    retryable = is_retryable(e)
                    delay = self.retry_policy.compute_delay(job.attempt) if retryable else 0.0
                    await self.queue.fail(
                        job.id,
                        job.fencing_token,
                        error=str(e),
                        retryable=retryable,
                        backoff_seconds=delay,
                        job_type=job.type,
                    )

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
            WORKERS_ACTIVE.inc(self.concurrency)
            for i in range(self.concurrency):
                task = asyncio.create_task(self._worker_loop(), name=f"{self.worker_id}-loop-{i}")
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def stop(self) -> None:
        """Gracefully stops worker."""
        if self._running:
            WORKERS_ACTIVE.dec(self.concurrency)
        self._running = False
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
