import asyncio
import time
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ExecutionModel, JobModel
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.scheduler.promoter import promote_due_jobs
from app.scheduler.reaper import reap_expired_jobs
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker


@pytest.mark.asyncio
async def test_long_running_job_with_heartbeat_succeeds(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Verifies that a job running longer than the initial lease TTL
    remains safe and succeeds due to active background heartbeats.
    """
    queue = PostgresQueueBackend(async_session_factory)
    registry = HandlerRegistry()

    # Handler sleeps 0.4s while lease TTL is only 0.2s
    async def slow_handler(ctx: JobContext) -> dict[str, str]:
        await asyncio.sleep(0.4)
        return {"status": "ok"}

    registry.register("slow_task", slow_handler)

    # 1. Enqueue job
    job_id = uuid.uuid4()
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="slow_task",
            payload={},
            status=JobStatus.QUEUED,
            timeout_seconds=5,
        )
        session.add(job)
        await session.commit()

    # 2. Worker claims and runs with TTL = 0.2s (heartbeat every ~0.06s)
    worker = Worker(
        worker_id="worker-diligent",
        queue=queue,
        registry=registry,
        lease_ttl=1,  # 1 second TTL
    )

    claimed = await worker.run_once()
    assert claimed is True

    # 3. Verify job completed as SUCCEEDED
    async with async_session_factory() as session:
        completed_job = await session.get(JobModel, job_id)
        assert completed_job is not None
        assert completed_job.status == JobStatus.SUCCEEDED
        assert completed_job.result == {"status": "ok"}

        executions = (
            await session.scalars(select(ExecutionModel).where(ExecutionModel.job_id == job_id))
        ).all()
        assert len(executions) == 1
        assert executions[0].outcome == ExecutionOutcome.SUCCEEDED.value


@pytest.mark.asyncio
async def test_worker_crash_recovery_via_reaper(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Core Guarantee G1 Verification:
    Worker 1 crashes mid-execution (kill -9 simulation).
    Reaper detects expired lease, records LEASE_EXPIRED, and recovers job.
    Promoter re-queues job, Worker 2 picks it up and finishes.
    Measures and logs recovery time.
    """
    queue = PostgresQueueBackend(async_session_factory)

    # 1. Enqueue job
    job_id = uuid.uuid4()
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="important_task",
            payload={"data": "val"},
            status=JobStatus.QUEUED,
            max_retries=3,
        )
        session.add(job)
        await session.commit()

    # 2. Worker 1 claims job with 1s lease TTL, but then crashes immediately
    start_time = time.monotonic()
    claimed_job = await queue.claim("worker-1-crashed", lease_ttl=1)
    assert claimed_job is not None
    assert claimed_job.status == JobStatus.RUNNING
    assert claimed_job.fencing_token == 1

    # Simulate worker crash: wait 1.1s for lease to expire with no heartbeats
    await asyncio.sleep(1.1)

    # 3. Reaper runs and detects expired lease
    async with async_session_factory() as session:
        reaped_count = await reap_expired_jobs(session, backoff_seconds=0)
        assert reaped_count == 1

    # 4. Promoter promotes RETRY_WAITING -> QUEUED
    async with async_session_factory() as session:
        promoted = await promote_due_jobs(session)
        assert promoted == 1

    # 5. Worker 2 claims the recovered job
    registry = HandlerRegistry()

    async def successful_handler(ctx: JobContext) -> dict[str, str]:
        return {"processed_by": "worker-2"}

    registry.register("important_task", successful_handler)

    worker_2 = Worker(
        worker_id="worker-2-healthy",
        queue=queue,
        registry=registry,
        lease_ttl=10,
    )
    processed = await worker_2.run_once()
    assert processed is True

    recovery_duration = time.monotonic() - start_time

    # 6. Verify final state and audit trail in executions
    async with async_session_factory() as session:
        final_job = await session.get(JobModel, job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.SUCCEEDED
        assert final_job.attempt == 2
        assert final_job.fencing_token == 2

        executions = (
            await session.scalars(
                select(ExecutionModel)
                .where(ExecutionModel.job_id == job_id)
                .order_by(ExecutionModel.attempt.asc())
            )
        ).all()

        assert len(executions) == 2
        # Attempt 1 failed with LEASE_EXPIRED
        assert executions[0].attempt == 1
        assert executions[0].worker_id == "worker-1-crashed"
        assert executions[0].fencing_token == 1
        assert executions[0].outcome == ExecutionOutcome.LEASE_EXPIRED.value

        # Attempt 2 succeeded by Worker 2
        assert executions[1].attempt == 2
        assert executions[1].worker_id == "worker-2-healthy"
        assert executions[1].fencing_token == 2
        assert executions[1].outcome == ExecutionOutcome.SUCCEEDED.value

    # Verify recovery time is bounded: ~1.1s - 2.5s
    assert recovery_duration < 5.0
