import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobModel
from app.domain.errors import NonRetryableError
from app.domain.retry_policy import RetryPolicy
from app.domain.state_machine import JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.scheduler.promoter import promote_due_jobs
from app.scheduler.reaper import reap_expired_jobs
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker


@pytest.mark.asyncio
async def test_poison_pill_progresses_through_retries_into_dead_letter(
    test_engine,
) -> None:
    """
    Core Guarantee G3 Verification:
    A faulty job (poison pill) retries through exponential backoffs up to max_retries,
    then permanently lands in DEAD_LETTER. The queue never retries indefinitely.
    """
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    call_count = 0

    @registry.register("poison_task")
    async def always_fails(ctx: JobContext) -> None:
        nonlocal call_count
        call_count += 1
        raise ConnectionError(f"Flaky service unavailable (call #{call_count})")

    # 1. Enqueue job with max_retries = 3 and 0s backoff policy for test speed
    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="poison_task",
            payload={"action": "test"},
            status=JobStatus.QUEUED,
            max_retries=3,
            run_at=now - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    fast_policy = RetryPolicy(base_delay=0.01, max_delay=0.05, full_jitter=False)
    worker = Worker(
        worker_id="test-dlq-worker",
        queue=backend,
        registry=registry,
        retry_policy=fast_policy,
    )

    # --- Attempt 1 ---
    claimed_1 = await worker.run_once()
    assert claimed_1 is True
    assert call_count == 1

    async with session_factory() as session:
        j1 = await session.get(JobModel, job_id)
        assert j1 is not None
        assert j1.status == JobStatus.RETRY_WAITING
        assert j1.attempt == 1
        # Fast-forward run_at for promotion
        j1.run_at = now - timedelta(seconds=1)
        await session.commit()

    async with session_factory() as session:
        promoted = await promote_due_jobs(session)
        assert promoted == 1

    # --- Attempt 2 ---
    claimed_2 = await worker.run_once()
    assert claimed_2 is True
    assert call_count == 2

    async with session_factory() as session:
        j2 = await session.get(JobModel, job_id)
        assert j2 is not None
        assert j2.status == JobStatus.RETRY_WAITING
        assert j2.attempt == 2
        # Fast-forward run_at for promotion
        j2.run_at = now - timedelta(seconds=1)
        await session.commit()

    async with session_factory() as session:
        promoted = await promote_due_jobs(session)
        assert promoted == 1

    # --- Attempt 3 (Final attempt: attempt >= max_retries) ---
    claimed_3 = await worker.run_once()
    assert claimed_3 is True
    assert call_count == 3

    async with session_factory() as session:
        j3 = await session.get(JobModel, job_id)
        assert j3 is not None
        # Must have transitioned to DEAD_LETTER!
        assert j3.status == JobStatus.DEAD_LETTER
        assert j3.attempt == 3
        assert "call #3" in (j3.last_error or "")

    # Verify Promoter never touches DEAD_LETTER
    async with session_factory() as session:
        promoted = await promote_due_jobs(session)
        assert promoted == 0

    # Verify Worker cannot claim DEAD_LETTER
    claimed_again = await worker.run_once()
    assert claimed_again is False


@pytest.mark.asyncio
async def test_non_retryable_error_bypasses_retries_straight_to_dead_letter(
    test_engine,
) -> None:
    """Verifies that NonRetryableError skips retries and immediately moves to DEAD_LETTER."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    @registry.register("invalid_payload_task")
    async def bad_payload(ctx: JobContext) -> None:
        raise NonRetryableError("Payload schema validation failed: missing key 'user_id'")

    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="invalid_payload_task",
            payload={},
            status=JobStatus.QUEUED,
            max_retries=5,  # High max_retries
            run_at=now - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="worker-fatal", queue=backend, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    async with session_factory() as session:
        dead_job = await session.get(JobModel, job_id)
        assert dead_job is not None
        assert dead_job.status == JobStatus.DEAD_LETTER
        assert dead_job.attempt == 1
        assert "Payload schema validation failed" in (dead_job.last_error or "")


@pytest.mark.asyncio
async def test_unregistered_handler_goes_directly_to_dead_letter(test_engine) -> None:
    """Verifies that jobs with unknown handler types fail immediately to DEAD_LETTER."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    empty_registry = HandlerRegistry()

    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="non_existent_type",
            payload={},
            status=JobStatus.QUEUED,
            max_retries=3,
            run_at=now - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="worker-missing", queue=backend, registry=empty_registry)
    claimed = await worker.run_once()
    assert claimed is True

    async with session_factory() as session:
        failed_job = await session.get(JobModel, job_id)
        assert failed_job is not None
        assert failed_job.status == JobStatus.DEAD_LETTER
        assert "No registered handler" in (failed_job.last_error or "")


@pytest.mark.asyncio
async def test_reaper_timeout_progresses_through_retries_to_dead_letter(
    test_engine,
) -> None:
    """Verifies that hung jobs reaped for TIMED_OUT follow backoff and transition to DEAD_LETTER."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    # Handler that hangs forever
    @registry.register("hang_forever")
    async def hang(ctx: JobContext) -> None:
        await ctx.sleep(100.0)

    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="hang_forever",
            payload={},
            status=JobStatus.QUEUED,
            max_retries=2,
            timeout_seconds=1,
            run_at=now - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    # Worker claims attempt 1
    claimed = await backend.claim("worker-1", lease_ttl=1)
    assert claimed is not None
    assert claimed.attempt == 1

    # Force expiration of execution deadline
    async with session_factory() as session:
        j = await session.get(JobModel, job_id)
        assert j is not None
        j.execution_deadline = now - timedelta(seconds=5)
        await session.commit()

    # Reaper runs -> attempt 1 < max_retries 2 -> RETRY_WAITING
    async with session_factory() as session:
        reaped = await reap_expired_jobs(session, backoff_seconds=0)
        assert reaped == 1

    async with session_factory() as session:
        j = await session.get(JobModel, job_id)
        assert j is not None
        assert j.status == JobStatus.RETRY_WAITING
        assert j.attempt == 1

    # Promote to QUEUED
    async with session_factory() as session:
        await promote_due_jobs(session)

    # Worker claims attempt 2
    claimed_2 = await backend.claim("worker-2", lease_ttl=1)
    assert claimed_2 is not None
    assert claimed_2.attempt == 2

    # Force expiration of execution deadline again
    async with session_factory() as session:
        j = await session.get(JobModel, job_id)
        assert j is not None
        j.execution_deadline = now - timedelta(seconds=5)
        await session.commit()

    # Reaper runs -> attempt 2 >= max_retries 2 -> DEAD_LETTER!
    async with session_factory() as session:
        reaped_2 = await reap_expired_jobs(session, backoff_seconds=0)
        assert reaped_2 == 1

    async with session_factory() as session:
        final_job = await session.get(JobModel, job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.DEAD_LETTER
        assert "TIMED_OUT" in (final_job.last_error or "")
