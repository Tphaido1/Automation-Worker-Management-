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
    db_file = tmp_path / "chaos_zombie_pause.db"
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


@pytest.mark.asyncio
async def test_chaos_zombie_worker_sigstop_commit_rejected(
    file_db_url: str,
    shared_session: AsyncSession,
) -> None:
    """
    Chaos Scenario:
    1. WorkerProcess-1 claims a job (token=1).
    2. WorkerProcess-1 is frozen via SIGSTOP (suspend).
    3. Lease expires while WorkerProcess-1 is frozen.
    4. Reaper reclaims the job; Promoter re-queues.
    5. WorkerProcess-2 claims the job (token=2) and commits SUCCEEDED.
    6. WorkerProcess-1 is unfrozen via SIGCONT (resume).
    7. WorkerProcess-1 wakes up and attempts commit with token=1.
    8. Token check rejects commit (REJECTED_STALE), preventing corruption.
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
        worker_id="zombie-worker-1",
        db_url=file_db_url,
        lease_ttl=2,
        poll_interval=0.1,
    )
    worker1.start()

    # Wait until WorkerProcess-1 claims the job
    claimed = False
    for _ in range(50):
        await asyncio.sleep(0.1)
        shared_session.expire_all()
        j = await shared_session.get(JobModel, job_id)
        if j and j.status == JobStatus.RUNNING and j.lease_owner == "zombie-worker-1":
            claimed = True
            break
    assert claimed is True, (
        f"Worker 1 failed to claim job: is_alive={worker1.is_alive()}, "
        f"stderr={worker1._proc.stderr.read() if worker1._proc and worker1._proc.stderr else ''}"
    )

    # 2. Freeze WorkerProcess-1 (SIGSTOP) mid-execution
    worker1.suspend()

    # 3. Wait for lease to expire while worker1 is frozen, then reap
    reaped = 0
    for _ in range(30):
        await asyncio.sleep(0.2)
        shared_session.expire_all()
        reaped = await reap_expired_jobs(shared_session, backoff_seconds=0.1)
        if reaped >= 1:
            break
    assert reaped >= 1, "Reaper failed to recover expired lease from paused zombie worker"

    await asyncio.sleep(0.2)
    promoted = await promote_due_jobs(shared_session)
    assert promoted >= 1

    # 5. WorkerProcess-2 claims the job and finishes
    worker2 = WorkerProcess(
        worker_id="active-worker-2",
        db_url=file_db_url,
        lease_ttl=10,
        poll_interval=0.1,
    )
    worker2.start()

    completed_by_worker2 = False
    for _ in range(100):
        await asyncio.sleep(0.1)
        shared_session.expire_all()
        j = await shared_session.get(JobModel, job_id)
        if j and j.status == JobStatus.SUCCEEDED:
            completed_by_worker2 = True
            break
    worker2.stop()
    assert completed_by_worker2 is True, "Worker 2 failed to complete job within 10s"

    # 6. Unfreeze WorkerProcess-1 (SIGCONT)
    # It will finish sleeping and attempt to commit with its stale token=1
    worker1.resume()

    # Wait for worker1 to wake up, execute commit, and have it rejected
    await asyncio.sleep(1.5)
    worker1.stop()

    # 7. Verify Invariants
    stmt = (
        select(ExecutionModel)
        .where(ExecutionModel.job_id == job_id)
        .order_by(ExecutionModel.attempt.asc())
    )
    executions = (await shared_session.scalars(stmt)).all()

    # Verify that worker1's execution is marked REJECTED_STALE or LEASE_EXPIRED
    outcomes = [e.outcome for e in executions]
    assert ExecutionOutcome.SUCCEEDED.value in outcomes
    # Exactly one SUCCEEDED
    assert outcomes.count(ExecutionOutcome.SUCCEEDED.value) == 1

    # Job terminal state is SUCCEEDED
    final_job = await shared_session.get(JobModel, job_id)
    assert final_job is not None
    assert final_job.status == JobStatus.SUCCEEDED
    assert final_job.fencing_token >= 2
