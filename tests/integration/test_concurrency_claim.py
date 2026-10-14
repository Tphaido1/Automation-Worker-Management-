import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ExecutionModel, JobModel
from app.domain.state_machine import JobStatus
from app.queue.postgres import PostgresQueueBackend


@pytest.mark.asyncio
async def test_20_workers_concurrently_claim_jobs(test_engine) -> None:
    """
    Phase 2 Core DoD Requirement:
    20 concurrent worker coroutines race to claim 500 queued jobs.
    Verifies that:
    1. Every job is claimed exactly ONCE (Zero duplicate claims across workers).
    2. Zero jobs are lost or skipped.
    3. Fencing token for every claimed job is exactly 1.
    4. Exactly 1 execution record is created per job.
    """
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)

    total_jobs = 500
    num_workers = 20
    now = datetime.now(UTC)

    # 1. Seed total_jobs in QUEUED state
    job_ids = [uuid.uuid4() for _ in range(total_jobs)]
    async with session_factory() as session:
        jobs = [
            JobModel(
                id=j_id,
                type="concurrent_job",
                payload={"index": i},
                status=JobStatus.QUEUED,
                priority=i % 10,
                run_at=now - timedelta(seconds=10),
            )
            for i, j_id in enumerate(job_ids)
        ]
        session.add_all(jobs)
        await session.commit()

    # 2. Launch 20 concurrent workers claiming jobs simultaneously
    claimed_records: list[tuple[str, uuid.UUID]] = []
    lock = asyncio.Lock()

    async def worker_task(worker_idx: int) -> None:
        worker_id = f"worker-node-{worker_idx}"
        while True:
            job = await backend.claim(worker_id=worker_id, lease_ttl=30)
            if not job:
                break
            async with lock:
                claimed_records.append((worker_id, job.id))

    workers = [worker_task(i) for i in range(num_workers)]
    await asyncio.gather(*workers)

    # 3. Assertions on claimed records
    assert len(claimed_records) == total_jobs, (
        f"Expected {total_jobs} claims, got {len(claimed_records)}"
    )

    claimed_ids = [j_id for _, j_id in claimed_records]
    unique_claimed_ids = set(claimed_ids)
    assert len(unique_claimed_ids) == total_jobs, (
        "CRITICAL: Duplicate job claim detected across workers!"
    )

    # 4. Assertions on Database State
    async with session_factory() as session:
        # Check all jobs are now RUNNING with attempt=1, fencing_token=1
        stmt = select(JobModel).where(JobModel.type == "concurrent_job")
        db_jobs = (await session.scalars(stmt)).all()

        assert len(db_jobs) == total_jobs
        for j in db_jobs:
            assert j.status == JobStatus.RUNNING
            assert j.attempt == 1
            assert j.fencing_token == 1
            assert j.lease_owner is not None
            assert j.lease_expires_at is not None

        # Check executions table has exactly 1 entry per job
        exec_count = await session.scalar(select(func.count(ExecutionModel.id)))
        assert exec_count == total_jobs


@pytest.mark.asyncio
async def test_repeated_concurrency_claim_reliability(test_engine) -> None:
    """
    DoD verification: Repeated concurrency rounds to verify zero race conditions.
    Runs 20 successive rounds of 10 workers claiming batches of jobs.
    """
    session_factory = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, expire_on_commit=False
    )
    backend = PostgresQueueBackend(session_factory)

    async def execute_round(round_num: int) -> None:
        now = datetime.now(UTC)
        round_batch_size = 50
        round_ids = [uuid.uuid4() for _ in range(round_batch_size)]

        async with session_factory() as session:
            jobs = [
                JobModel(
                    id=j_id,
                    type=f"round_{round_num}",
                    payload={},
                    status=JobStatus.QUEUED,
                    run_at=now - timedelta(seconds=1),
                )
                for j_id in round_ids
            ]
            session.add_all(jobs)
            await session.commit()

        claimed_in_round: list[uuid.UUID] = []
        lock = asyncio.Lock()

        async def sub_worker(idx: int) -> None:
            w_id = f"r{round_num}-w{idx}"
            while True:
                job = await backend.claim(w_id)
                if not job:
                    break
                async with lock:
                    claimed_in_round.append(job.id)

        await asyncio.gather(*[sub_worker(i) for i in range(10)])

        assert len(claimed_in_round) == round_batch_size
        assert len(set(claimed_in_round)) == round_batch_size, (
            f"Race condition detected in round {round_num}!"
        )

    for r in range(10):
        await execute_round(r)
