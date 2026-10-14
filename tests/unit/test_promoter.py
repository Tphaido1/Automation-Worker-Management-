import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobModel
from app.domain.state_machine import JobStatus
from app.scheduler.promoter import promote_due_jobs


@pytest.mark.asyncio
async def test_promoter_promotes_only_due_jobs(test_session: AsyncSession) -> None:
    """Verifies that promoter only promotes PENDING jobs whose run_at <= now."""
    now = datetime.now(UTC)

    # 1. Past job (due immediately)
    job_past = JobModel(
        id=uuid.uuid4(),
        type="test_past",
        payload={},
        status=JobStatus.PENDING,
        run_at=now - timedelta(minutes=5),
    )
    # 2. Future job (not due yet)
    job_future = JobModel(
        id=uuid.uuid4(),
        type="test_future",
        payload={},
        status=JobStatus.PENDING,
        run_at=now + timedelta(hours=1),
    )
    # 3. Already QUEUED job
    job_queued = JobModel(
        id=uuid.uuid4(),
        type="test_queued",
        payload={},
        status=JobStatus.QUEUED,
        run_at=now - timedelta(minutes=2),
    )

    test_session.add_all([job_past, job_future, job_queued])
    await test_session.commit()

    # Run promotion
    promoted_count = await promote_due_jobs(test_session)
    assert promoted_count == 1

    # Verify states
    refreshed_past = await test_session.get(JobModel, job_past.id)
    refreshed_future = await test_session.get(JobModel, job_future.id)
    refreshed_queued = await test_session.get(JobModel, job_queued.id)

    assert refreshed_past is not None
    assert refreshed_past.status == JobStatus.QUEUED

    assert refreshed_future is not None
    assert refreshed_future.status == JobStatus.PENDING

    assert refreshed_queued is not None
    assert refreshed_queued.status == JobStatus.QUEUED
