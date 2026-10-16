import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ExecutionModel, JobModel
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.scheduler.reaper import reap_expired_jobs


@pytest.mark.asyncio
async def test_reaper_reclaims_expired_lease(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        # Create a RUNNING job with an expired lease
        job = JobModel(
            id=job_id,
            type="send_email",
            payload={"to": "user@example.com"},
            status=JobStatus.RUNNING,
            fencing_token=1,
            attempt=1,
            max_retries=3,
            lease_owner="worker-dead",
            lease_expires_at=now - timedelta(seconds=10),
            execution_deadline=now + timedelta(seconds=50),
        )
        session.add(job)
        execution = ExecutionModel(
            job_id=job_id,
            attempt=1,
            worker_id="worker-dead",
            fencing_token=1,
            started_at=now - timedelta(seconds=40),
        )
        session.add(execution)
        await session.commit()

    # Run reaper
    async with async_session_factory() as session:
        reaped = await reap_expired_jobs(session)
        assert reaped == 1

    # Verify state transitions
    async with async_session_factory() as session:
        reaped_job = await session.get(JobModel, job_id)
        assert reaped_job is not None
        assert reaped_job.status == JobStatus.RETRY_WAITING
        assert reaped_job.lease_owner is None
        assert reaped_job.lease_expires_at is None
        assert "LEASE_EXPIRED" in (reaped_job.last_error or "")

        exec_record = (
            await session.scalars(
                select(ExecutionModel).where(
                    ExecutionModel.job_id == job_id,
                    ExecutionModel.fencing_token == 1,
                )
            )
        ).one()
        assert exec_record.outcome == ExecutionOutcome.LEASE_EXPIRED.value
        assert exec_record.finished_at is not None


@pytest.mark.asyncio
async def test_reaper_reclaims_timed_out_job(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        # Job has valid lease, but exceeded execution deadline
        job = JobModel(
            id=job_id,
            type="heavy_computation",
            payload={},
            status=JobStatus.RUNNING,
            fencing_token=2,
            attempt=1,
            max_retries=3,
            lease_owner="worker-alive",
            lease_expires_at=now + timedelta(seconds=20),
            execution_deadline=now - timedelta(seconds=5),
        )
        session.add(job)
        execution = ExecutionModel(
            job_id=job_id,
            attempt=1,
            worker_id="worker-alive",
            fencing_token=2,
            started_at=now - timedelta(seconds=100),
        )
        session.add(execution)
        await session.commit()

    async with async_session_factory() as session:
        reaped = await reap_expired_jobs(session)
        assert reaped == 1

    async with async_session_factory() as session:
        reaped_job = await session.get(JobModel, job_id)
        assert reaped_job is not None
        assert reaped_job.status == JobStatus.RETRY_WAITING
        assert "TIMED_OUT" in (reaped_job.last_error or "")

        exec_record = (
            await session.scalars(
                select(ExecutionModel).where(
                    ExecutionModel.job_id == job_id,
                    ExecutionModel.fencing_token == 2,
                )
            )
        ).one()
        assert exec_record.outcome == ExecutionOutcome.TIMED_OUT.value


@pytest.mark.asyncio
async def test_reaper_moves_to_dead_letter_when_max_retries_exceeded(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="poison_pill",
            payload={},
            status=JobStatus.RUNNING,
            fencing_token=3,
            attempt=3,  # Already at max_retries
            max_retries=3,
            lease_owner="worker-1",
            lease_expires_at=now - timedelta(seconds=1),
        )
        session.add(job)
        execution = ExecutionModel(
            job_id=job_id,
            attempt=3,
            worker_id="worker-1",
            fencing_token=3,
            started_at=now - timedelta(seconds=30),
        )
        session.add(execution)
        await session.commit()

    async with async_session_factory() as session:
        reaped = await reap_expired_jobs(session)
        assert reaped == 1

    async with async_session_factory() as session:
        reaped_job = await session.get(JobModel, job_id)
        assert reaped_job is not None
        assert reaped_job.status == JobStatus.DEAD_LETTER


@pytest.mark.asyncio
async def test_reaper_ignores_active_valid_leases(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="normal_job",
            payload={},
            status=JobStatus.RUNNING,
            fencing_token=1,
            attempt=1,
            max_retries=3,
            lease_owner="worker-active",
            lease_expires_at=now + timedelta(seconds=30),
            execution_deadline=now + timedelta(seconds=60),
        )
        session.add(job)
        await session.commit()

    async with async_session_factory() as session:
        reaped = await reap_expired_jobs(session)
        assert reaped == 0

    async with async_session_factory() as session:
        unchanged_job = await session.get(JobModel, job_id)
        assert unchanged_job is not None
        assert unchanged_job.status == JobStatus.RUNNING
