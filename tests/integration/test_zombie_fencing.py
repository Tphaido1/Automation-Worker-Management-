import asyncio
import random
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ExecutionModel, JobModel
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.scheduler.promoter import promote_due_jobs
from app.scheduler.reaper import reap_expired_jobs


@pytest.mark.asyncio
async def test_zombie_worker_commit_rejected_by_fencing_token(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Phase 4 Core DoD Requirement (Guarantee G2: Không ghi đè kép):
    1. Worker A claims job (token 1).
    2. Worker A pauses (simulated process freeze / SIGSTOP / network partition).
    3. Lease expires, Reaper reclaims job (LEASE_EXPIRED), Promoter re-queues.
    4. Worker B claims job (token 2) and commits successfully (result B).
    5. Worker A resumes and tries to commit with stale token 1.
    6. Expectations:
       - A's commit returns False.
       - B's result remains completely intact in jobs table.
       - Executions table records REJECTED_STALE for token 1.
       - Executions table records SUCCEEDED for token 2.
    """
    queue = PostgresQueueBackend(async_session_factory)

    # 1. Enqueue job
    job_id = uuid.uuid4()
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="data_sync",
            payload={"source": "upstream"},
            status=JobStatus.QUEUED,
            max_retries=3,
        )
        session.add(job)
        await session.commit()

    # 2. Worker A claims job with 1s lease TTL
    claimed_a = await queue.claim("worker-a", lease_ttl=1)
    assert claimed_a is not None
    assert claimed_a.fencing_token == 1
    assert claimed_a.status == JobStatus.RUNNING

    # 3. Worker A pauses: wait 1.1s for lease to expire without heartbeats
    await asyncio.sleep(1.1)

    # 4. Reaper reclaims the job
    async with async_session_factory() as session:
        reaped = await reap_expired_jobs(session, backoff_seconds=0)
        assert reaped == 1

    # 5. Promoter re-queues the job
    async with async_session_factory() as session:
        promoted = await promote_due_jobs(session)
        assert promoted == 1

    # 6. Worker B claims the job with incremented token 2
    claimed_b = await queue.claim("worker-b", lease_ttl=10)
    assert claimed_b is not None
    assert claimed_b.fencing_token == 2
    assert claimed_b.attempt == 2

    # 7. Worker B finishes and commits result
    b_success = await queue.complete(
        job_id=job_id,
        fencing_token=2,
        result={"author": "worker-b", "status": "completed"},
    )
    assert b_success is True

    # 8. Worker A wakes up and attempts to commit stale result with token 1
    a_success = await queue.complete(
        job_id=job_id,
        fencing_token=1,
        result={"author": "worker-a-zombie", "status": "corrupted"},
    )
    assert a_success is False, "Stale commit with token 1 MUST be rejected"

    # 9. Verify database integrity
    async with async_session_factory() as session:
        final_job = await session.get(JobModel, job_id)
        assert final_job is not None
        assert final_job.status == JobStatus.SUCCEEDED
        assert final_job.fencing_token == 2
        # Core Guarantee G2: B's result is protected from zombie overwrite
        assert final_job.result == {"author": "worker-b", "status": "completed"}

        # Audit trail verification in executions
        executions = (
            await session.scalars(
                select(ExecutionModel)
                .where(ExecutionModel.job_id == job_id)
                .order_by(ExecutionModel.attempt.asc())
            )
        ).all()

        assert len(executions) == 2
        # Execution 1 (Worker A) was rejected as stale
        assert executions[0].worker_id == "worker-a"
        assert executions[0].fencing_token == 1
        assert executions[0].outcome == ExecutionOutcome.REJECTED_STALE.value

        # Execution 2 (Worker B) succeeded
        assert executions[1].worker_id == "worker-b"
        assert executions[1].fencing_token == 2
        assert executions[1].outcome == ExecutionOutcome.SUCCEEDED.value


@pytest.mark.asyncio
async def test_stale_worker_heartbeat_and_fail_are_rejected(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Verifies that all worker operations (heartbeat and fail) are strictly
    fenced by the monotonic fencing token.
    """
    queue = PostgresQueueBackend(async_session_factory)
    job_id = uuid.uuid4()

    # Create job directly in RUNNING state with fencing_token = 5
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="fenced_ops",
            payload={},
            status=JobStatus.RUNNING,
            fencing_token=5,
            attempt=1,
            lease_owner="worker-current",
        )
        session.add(job)
        await session.commit()

    # 1. Stale token 4 cannot extend heartbeat
    hb_stale = await queue.heartbeat(job_id=job_id, fencing_token=4, lease_ttl=30)
    assert hb_stale is False

    # 2. Valid token 5 extends heartbeat
    hb_valid = await queue.heartbeat(job_id=job_id, fencing_token=5, lease_ttl=30)
    assert hb_valid is True

    # 3. Stale token 4 cannot fail the job
    fail_stale = await queue.fail(job_id=job_id, fencing_token=4, error="Zombie error")
    assert fail_stale is False

    async with async_session_factory() as session:
        job_check = await session.get(JobModel, job_id)
        assert job_check is not None
        assert job_check.status == JobStatus.RUNNING  # Not failed to DEAD_LETTER


@pytest.mark.asyncio
async def test_repeated_zombie_rejection_reliability(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Runs 50 repeated cycles of zombie worker race conditions with random jitter
    to prove zero race conditions or accidental double commits.
    """
    queue = PostgresQueueBackend(async_session_factory)

    for cycle in range(50):
        job_id = uuid.uuid4()
        async with async_session_factory() as session:
            job = JobModel(
                id=job_id,
                type="race_test",
                payload={"cycle": cycle},
                status=JobStatus.QUEUED,
            )
            session.add(job)
            await session.commit()

        # Worker 1 claims
        c1 = await queue.claim("worker-1", lease_ttl=1)
        assert c1 is not None

        # Simulate reaper reclaiming without waiting full duration
        async with async_session_factory() as session:
            db_job = await session.get(JobModel, job_id)
            assert db_job is not None
            db_job.status = JobStatus.QUEUED
            db_job.lease_owner = None
            db_job.lease_expires_at = None
            await session.commit()

        # Worker 2 claims with token 2
        c2 = await queue.claim("worker-2", lease_ttl=10)
        assert c2 is not None
        assert c2.fencing_token == 2

        # Concurrently commit: Worker 1 (stale token 1) and Worker 2 (token 2) with jitter
        jitter = random.uniform(0.001, 0.01)

        async def worker_1_commit(j_id: uuid.UUID = job_id, sleep_time: float = jitter) -> bool:
            await asyncio.sleep(sleep_time)
            return await queue.complete(j_id, fencing_token=1, result={"winner": 1})

        async def worker_2_commit(j_id: uuid.UUID = job_id) -> bool:
            await asyncio.sleep(random.uniform(0.001, 0.01))
            return await queue.complete(j_id, fencing_token=2, result={"winner": 2})

        r1, r2 = await asyncio.gather(worker_1_commit(), worker_2_commit())

        # Worker 2 must succeed; Worker 1 must be rejected
        assert r2 is True, f"Cycle {cycle}: Worker 2 valid commit failed"
        assert r1 is False, f"Cycle {cycle}: Worker 1 zombie commit was erroneously accepted"

        async with async_session_factory() as session:
            final_job = await session.get(JobModel, job_id)
            assert final_job is not None
            assert final_job.result == {"winner": 2}
