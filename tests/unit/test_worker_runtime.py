import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobModel
from app.domain.state_machine import JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker


@pytest.mark.asyncio
async def test_worker_executes_async_handler(test_engine) -> None:
    """Verifies that worker claims job, invokes async handler, and commits SUCCEEDED."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    handled_payloads = []

    @registry.register("send_webhook")
    async def handle_webhook(ctx: JobContext) -> dict:
        handled_payloads.append(ctx.payload)
        return {"delivered": True, "http_status": 200}

    # Setup ready job
    job_id = uuid.uuid4()
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="send_webhook",
            payload={"url": "https://api.example.com/notify"},
            status=JobStatus.QUEUED,
            run_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="test-worker-1", queue=backend, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True
    assert len(handled_payloads) == 1
    assert handled_payloads[0]["url"] == "https://api.example.com/notify"

    # Verify job state in DB
    async with session_factory() as session:
        completed_job = await session.get(JobModel, job_id)
        assert completed_job is not None
        assert completed_job.status == JobStatus.SUCCEEDED
        assert completed_job.result == {"delivered": True, "http_status": 200}
        assert completed_job.fencing_token == 1
        assert completed_job.attempt == 1


@pytest.mark.asyncio
async def test_worker_handles_sync_function(test_engine) -> None:
    """Verifies that synchronous handlers are executed safely via to_thread."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    @registry.register("compute_hash")
    def sync_hash(ctx: JobContext) -> str:
        # Synchronous CPU-bound simulation
        return f"hash_of_{ctx.payload.get('text')}"

    job_id = uuid.uuid4()
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="compute_hash",
            payload={"text": "hello_leaseflow"},
            status=JobStatus.QUEUED,
            run_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="test-worker-2", queue=backend, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    async with session_factory() as session:
        completed_job = await session.get(JobModel, job_id)
        assert completed_job is not None
        assert completed_job.status == JobStatus.SUCCEEDED
        assert completed_job.result == {"output": "hash_of_hello_leaseflow"}


@pytest.mark.asyncio
async def test_worker_records_handler_exception(test_engine) -> None:
    """Verifies that an unhandled error inside handler transitions job and records error."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    @registry.register("faulty_task")
    async def faulty_task(ctx: JobContext) -> None:
        raise ValueError("Simulated network timeout")

    job_id = uuid.uuid4()
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="faulty_task",
            payload={},
            status=JobStatus.QUEUED,
            max_retries=1,  # Exhausts retries on attempt 1
            run_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="test-worker-3", queue=backend, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    async with session_factory() as session:
        failed_job = await session.get(JobModel, job_id)
        assert failed_job is not None
        assert failed_job.status == JobStatus.DEAD_LETTER
        assert "Simulated network timeout" in (failed_job.last_error or "")


@pytest.mark.asyncio
async def test_worker_transitions_to_retry_waiting_when_retries_remain(test_engine) -> None:
    """Verifies that an unhandled retryable error sets job to RETRY_WAITING with future run_at."""
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)
    registry = HandlerRegistry()

    @registry.register("transient_fail")
    async def transient_fail(ctx: JobContext) -> None:
        raise ConnectionResetError("Connection lost to upstream service")

    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with session_factory() as session:
        job = JobModel(
            id=job_id,
            type="transient_fail",
            payload={},
            status=JobStatus.QUEUED,
            max_retries=3,
            run_at=now - timedelta(seconds=1),
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="test-worker-retry", queue=backend, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    async with session_factory() as session:
        retrying_job = await session.get(JobModel, job_id)
        assert retrying_job is not None
        assert retrying_job.status == JobStatus.RETRY_WAITING
        assert retrying_job.attempt == 1
        assert "Connection lost to upstream service" in (retrying_job.last_error or "")
        assert retrying_job.lease_owner is None
        assert retrying_job.lease_expires_at is None
