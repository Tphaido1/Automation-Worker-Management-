import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobModel
from app.domain.state_machine import JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker


@pytest.mark.asyncio
async def test_get_jobs_listing_and_filtering(
    async_client: AsyncClient,
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Verifies GET /jobs with status/type filtering and /dead-letters endpoint."""
    now = datetime.now(UTC)
    id1, id2, id3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    async with async_session_factory() as session:
        j1 = JobModel(
            id=id1,
            type="email_alert",
            payload={"recipient": "a@example.com"},
            status=JobStatus.QUEUED,
            run_at=now,
        )
        j2 = JobModel(
            id=id2,
            type="email_alert",
            payload={"recipient": "b@example.com"},
            status=JobStatus.DEAD_LETTER,
            last_error="SMTP server unreachable",
            run_at=now,
        )
        j3 = JobModel(
            id=id3,
            type="resize_avatar",
            payload={"width": 100},
            status=JobStatus.SUCCEEDED,
            run_at=now,
        )
        session.add_all([j1, j2, j3])
        await session.commit()

    # 1. GET /jobs (unfiltered)
    resp_all = await async_client.get("/jobs")
    assert resp_all.status_code == 200
    all_jobs = resp_all.json()
    job_ids = [j["id"] for j in all_jobs]
    assert str(id1) in job_ids
    assert str(id2) in job_ids
    assert str(id3) in job_ids

    # 2. GET /jobs?status=DEAD_LETTER
    resp_dl = await async_client.get("/jobs?status=DEAD_LETTER")
    assert resp_dl.status_code == 200
    dl_jobs = resp_dl.json()
    dl_ids = [j["id"] for j in dl_jobs]
    assert str(id2) in dl_ids
    assert str(id1) not in dl_ids
    assert str(id3) not in dl_ids

    # 3. GET /dead-letters (root alias)
    resp_root_dl = await async_client.get("/dead-letters")
    assert resp_root_dl.status_code == 200
    root_dl_ids = [j["id"] for j in resp_root_dl.json()]
    assert str(id2) in root_dl_ids
    assert str(id1) not in root_dl_ids

    # 4. GET /jobs/dead-letters (nested route)
    resp_nested_dl = await async_client.get("/jobs/dead-letters")
    assert resp_nested_dl.status_code == 200
    nested_dl_ids = [j["id"] for j in resp_nested_dl.json()]
    assert str(id2) in nested_dl_ids

    # 5. GET /jobs?type=resize_avatar
    resp_type = await async_client.get("/jobs?type=resize_avatar")
    assert resp_type.status_code == 200
    type_ids = [j["id"] for j in resp_type.json()]
    assert str(id3) in type_ids
    assert str(id1) not in type_ids


@pytest.mark.asyncio
async def test_manual_retry_dead_letter_job(
    async_client: AsyncClient,
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Verifies operator manually retrying a DEAD_LETTER job:
    1. Job is stuck in DEAD_LETTER.
    2. POST /jobs/{id}/retry transitions job to QUEUED, resets attempt counter.
    3. Worker claims job and successfully executes it.
    """
    job_id = uuid.uuid4()
    now = datetime.now(UTC)

    # 1. Seed job in DEAD_LETTER state
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="recoverable_task",
            payload={"attempts_so_far": 3},
            status=JobStatus.DEAD_LETTER,
            attempt=3,
            max_retries=3,
            last_error="Temporary external service outage",
            run_at=now,
        )
        session.add(job)
        await session.commit()

    # 2. Operator calls POST /jobs/{id}/retry
    retry_resp = await async_client.post(f"/jobs/{job_id}/retry")
    assert retry_resp.status_code == 200
    retried_data = retry_resp.json()
    assert retried_data["id"] == str(job_id)
    assert retried_data["status"] == "QUEUED"
    assert retried_data["attempt"] == 0

    # 3. Verify in database
    async with async_session_factory() as session:
        db_job = await session.get(JobModel, job_id)
        assert db_job is not None
        assert db_job.status == JobStatus.QUEUED
        assert db_job.attempt == 0
        assert db_job.last_error is None

    # 4. Worker now claims the resurrected job and executes to SUCCEEDED
    backend = PostgresQueueBackend(async_session_factory)
    registry = HandlerRegistry()

    @registry.register("recoverable_task")
    async def handle_recoverable(ctx: JobContext) -> dict[str, str]:
        return {"result": "recovered successfully"}

    worker = Worker(worker_id="recovery-worker", queue=backend, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    async with async_session_factory() as session:
        completed = await session.get(JobModel, job_id)
        assert completed is not None
        assert completed.status == JobStatus.SUCCEEDED
        assert completed.result == {"result": "recovered successfully"}


@pytest.mark.asyncio
async def test_retry_non_dead_letter_job_returns_400(
    async_client: AsyncClient,
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Verifies that attempting to retry a SUCCEEDED job returns HTTP 400 (illegal state transition)."""
    job_id = uuid.uuid4()
    now = datetime.now(UTC)

    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="completed_task",
            payload={},
            status=JobStatus.SUCCEEDED,
            run_at=now,
        )
        session.add(job)
        await session.commit()

    retry_resp = await async_client.post(f"/jobs/{job_id}/retry")
    assert retry_resp.status_code == 400
    assert "Invalid state transition" in retry_resp.json()["detail"]


@pytest.mark.asyncio
async def test_retry_nonexistent_job_returns_404(
    async_client: AsyncClient,
) -> None:
    """Verifies that attempting to retry a non-existent job returns HTTP 404."""
    non_existent_id = uuid.uuid4()
    retry_resp = await async_client.post(f"/jobs/{non_existent_id}/retry")
    assert retry_resp.status_code == 404
    assert f"Job with ID '{non_existent_id}' not found" in retry_resp.json()["detail"]
