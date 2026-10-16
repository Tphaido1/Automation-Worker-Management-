import asyncio
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.db.models import Base, ExecutionModel, JobModel
from app.db.repositories import JobRepository
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.scheduler.promoter import promote_due_jobs
from app.scheduler.reaper import reap_expired_jobs
from tests.chaos.harness import WorkerProcess


@pytest.fixture
def file_db_url(tmp_path: Path) -> Generator[str, None, None]:
    """Provides a shared file-based SQLite database configured with WAL mode for inter-process access."""
    db_file = tmp_path / "chaos_worker_kill.db"
    url = f"sqlite+aiosqlite:///{db_file.as_posix()}"

    async def init_schema() -> None:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("PRAGMA journal_mode=WAL;"))
        await engine.dispose()

    asyncio.run(init_schema())
    yield url


@pytest.fixture
async def shared_session(file_db_url: str) -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(file_db_url, connect_args={"check_same_thread": False})
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


@pytest.fixture
def shared_session_factory(file_db_url: str) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(file_db_url, connect_args={"check_same_thread": False})
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


@pytest.mark.asyncio
async def test_chaos_worker_sigkill_recovery(
    file_db_url: str,
    shared_session: AsyncSession,
    shared_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Chaos Scenario:
    1. A job is submitted.
    2. WorkerProcess-1 (real OS subprocess) claims and begins executing the job.
    3. While WorkerProcess-1 is running, it receives SIGKILL (process abruptly terminated).
    4. Worker lease expires.
    5. Reaper detects expired lease, closes execution with LEASE_EXPIRED, and re-queues.
    6. WorkerProcess-2 claims the recovered job and completes successfully.
    7. Verifies zero lost jobs, 1 successful execution, and correct outcome history.
    """
    repo = JobRepository(shared_session)
    job, _ = await repo.create_job(
        job_type="sleep_job",
        payload={"duration": 4.0},
        max_retries=3,
        timeout_seconds=30,
    )
    job.status = JobStatus.QUEUED
    await shared_session.commit()
    job_id = job.id

    # 1. Spawn WorkerProcess-1 with short lease TTL
    worker1 = WorkerProcess(
        worker_id="chaos-worker-1",
        db_url=file_db_url,
        lease_ttl=2,
        poll_interval=0.1,
    )
    worker1.start()

    # Wait until WorkerProcess-1 has claimed the job
    claimed = False
    for _ in range(50):
        await asyncio.sleep(0.1)
        shared_session.expire_all()
        j = await shared_session.get(JobModel, job_id)
        if j and j.status == JobStatus.RUNNING and j.lease_owner == "chaos-worker-1":
            claimed = True
            break
    assert claimed is True, "Worker 1 failed to claim job"

    # 2. Simulate sudden node crash / kill -9
    worker1.kill()
    assert not worker1.is_alive()

    # 3. Wait for lease to expire in DB (lease_ttl=2s)
    reaped = 0
    for _ in range(30):
        await asyncio.sleep(0.2)
        reaped = await reap_expired_jobs(shared_session, backoff_seconds=0.0)
        if reaped >= 1:
            break
    assert reaped >= 1, "Reaper failed to recover expired job"

    # 4. Promoter pushes job back to QUEUED
    promoted = 0
    for _ in range(20):
        promoted = await promote_due_jobs(shared_session)
        if promoted >= 1:
            break
        await asyncio.sleep(0.1)
    assert promoted >= 1, "Promoter failed to promote reaped job"

    # 5. Spawn WorkerProcess-2 to pick up the re-queued job
    worker2 = WorkerProcess(
        worker_id="chaos-worker-2",
        db_url=file_db_url,
        lease_ttl=10,
        poll_interval=0.1,
    )
    worker2.start()

    # Wait for WorkerProcess-2 to complete the job
    completed = False
    for _ in range(100):
        await asyncio.sleep(0.1)
        shared_session.expire_all()
        j = await shared_session.get(JobModel, job_id)
        if j and j.status == JobStatus.SUCCEEDED:
            completed = True
            break

    worker2.stop()
    assert completed is True, "Worker 2 failed to complete reaped job"

    # 6. Verify Execution invariants
    stmt = (
        select(ExecutionModel)
        .where(ExecutionModel.job_id == job_id)
        .order_by(ExecutionModel.attempt.asc())
    )
    executions = (await shared_session.scalars(stmt)).all()

    assert len(executions) == 2
    # Attempt 1: Killed worker -> LEASE_EXPIRED
    assert executions[0].worker_id == "chaos-worker-1"
    assert executions[0].outcome == ExecutionOutcome.LEASE_EXPIRED.value
    # Attempt 2: Recovered worker -> SUCCEEDED
    assert executions[1].worker_id == "chaos-worker-2"
    assert executions[1].outcome == ExecutionOutcome.SUCCEEDED.value
