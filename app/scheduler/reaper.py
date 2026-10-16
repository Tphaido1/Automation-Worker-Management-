import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.metrics import (
    DEAD_LETTER_TOTAL,
    JOBS_TOTAL,
    LEASE_EXPIRED_TOTAL,
    RECOVERY_SECONDS,
    RETRIES_TOTAL,
)
from app.db.models import ExecutionModel, JobModel
from app.domain.retry_policy import RetryPolicy, compute_delay
from app.domain.state_machine import ExecutionOutcome, JobStatus

logger = logging.getLogger(__name__)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


async def reap_expired_jobs(
    session: AsyncSession,
    backoff_seconds: float | None = None,
    retry_policy: RetryPolicy | None = None,
) -> int:
    """
    Identifies and reclaims RUNNING jobs that:
    1. Have expired leases (lease_expires_at <= now): Worker crashed or partitioned (ADR-4).
    2. Have exceeded execution deadlines (execution_deadline <= now): Job hung/deadlocked (ADR-4).

    For each reaped job:
    - Closes execution record with LEASE_EXPIRED or TIMED_OUT.
    - Transitions job to RETRY_WAITING (if attempt < max_retries) or DEAD_LETTER (if attempt >= max_retries).
    - Computes exponential backoff with full jitter (ADR-5) unless explicit backoff_seconds is passed.
    - Clears lease ownership.

    Returns the count of reaped jobs.
    """
    now = datetime.now(UTC)

    # Atomic candidate selection with SKIP LOCKED to prevent Reaper clashes
    stmt = (
        select(JobModel)
        .where(
            JobModel.status == JobStatus.RUNNING,
            or_(
                and_(JobModel.lease_expires_at.is_not(None), JobModel.lease_expires_at <= now),
                and_(JobModel.execution_deadline.is_not(None), JobModel.execution_deadline <= now),
            ),
        )
        .with_for_update(skip_locked=True)
    )

    reaped_jobs = (await session.scalars(stmt)).all()
    if not reaped_jobs:
        return 0

    count = 0
    for job in reaped_jobs:
        # Determine outcome: execution deadline violation takes precedence if both expired
        deadline = _to_utc(job.execution_deadline) if job.execution_deadline else None
        if deadline and deadline <= now:
            outcome = ExecutionOutcome.TIMED_OUT
            err_msg = f"[{outcome.value}] Job exceeded execution deadline of {job.timeout_seconds}s"
        else:
            outcome = ExecutionOutcome.LEASE_EXPIRED
            err_msg = (
                f"[{outcome.value}] Worker lease expired (lease_expires_at={job.lease_expires_at})"
            )

        # Determine target status according to retry limits
        if job.attempt >= job.max_retries:
            target_status = JobStatus.DEAD_LETTER
            retry_run_at = job.run_at
        else:
            target_status = JobStatus.RETRY_WAITING
            if backoff_seconds is not None:
                delay = float(backoff_seconds)
            elif retry_policy is not None:
                delay = retry_policy.compute_delay(job.attempt)
            else:
                delay = compute_delay(job.attempt)
            retry_run_at = now + timedelta(seconds=delay)

        # Update Job state
        job.status = target_status
        job.lease_owner = None
        job.lease_expires_at = None
        job.execution_deadline = None
        job.run_at = retry_run_at
        job.last_error = err_msg
        job.updated_at = now

        # Update corresponding Execution record
        exec_stmt = (
            update(ExecutionModel)
            .where(
                ExecutionModel.job_id == job.id,
                ExecutionModel.fencing_token == job.fencing_token,
                ExecutionModel.finished_at.is_(None),
            )
            .values(
                finished_at=now,
                outcome=outcome.value,
                error=err_msg,
            )
        )
        await session.execute(exec_stmt)
        count += 1

        # Record reaper observability metrics
        LEASE_EXPIRED_TOTAL.labels(type=job.type).inc()
        if target_status == JobStatus.DEAD_LETTER:
            DEAD_LETTER_TOTAL.labels(type=job.type).inc()
            JOBS_TOTAL.labels(type=job.type, status=JobStatus.DEAD_LETTER.value).inc()
        else:
            RETRIES_TOTAL.labels(type=job.type).inc()
            JOBS_TOTAL.labels(type=job.type, status=JobStatus.RETRY_WAITING.value).inc()

        ref_expiry = (
            deadline
            if (deadline and deadline <= now)
            else (_to_utc(job.lease_expires_at) if job.lease_expires_at else now)
        )
        recovery_time = max(0.0, (now - ref_expiry).total_seconds())
        RECOVERY_SECONDS.observe(recovery_time)

        logger.warning(
            "Reaped job '%s' (token=%d, attempt=%d/%d, outcome=%s -> %s)",
            job.id,
            job.fencing_token,
            job.attempt,
            job.max_retries,
            outcome.value,
            target_status.value,
        )

    await session.commit()
    return count


class ReaperLoop:
    """Background service that periodically runs reap_expired_jobs."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        interval_seconds: float = 3.0,
        backoff_seconds: float | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.interval_seconds = interval_seconds
        self.backoff_seconds = backoff_seconds
        self.retry_policy = retry_policy
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> int:
        """Runs a single reaping cycle."""
        async with self.session_factory() as session:
            return await reap_expired_jobs(
                session,
                backoff_seconds=self.backoff_seconds,
                retry_policy=self.retry_policy,
            )

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Error in reaper loop: %s", e, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="leaseflow-reaper")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
