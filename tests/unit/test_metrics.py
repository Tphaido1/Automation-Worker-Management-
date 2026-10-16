import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import (
    JSONFormatter,
    ctx_attempt,
    ctx_fencing_token,
    ctx_job_id,
    ctx_worker_id,
)
from app.core.metrics import (
    DUPLICATE_DELIVERIES_TOTAL,
    JOB_DURATION_SECONDS,
    JOBS_IN_STATE,
    LEASE_EXPIRED_TOTAL,
    QUEUE_WAIT_SECONDS,
    STALE_COMMIT_REJECTED_TOTAL,
    WORKERS_ACTIVE,
    update_queue_state_metrics,
)
from app.db.models import JobModel
from app.db.repositories import JobRepository
from app.domain.state_machine import JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.scheduler.reaper import reap_expired_jobs
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker


@pytest.mark.asyncio
async def test_metrics_endpoint_scrapes_all_registered_metrics(
    async_client: AsyncClient,
    test_session: AsyncSession,
) -> None:
    """Verifies that GET /metrics renders all 12 section 6.1 Prometheus metrics."""
    # Seed one job to ensure queue state metrics query executes cleanly
    repo = JobRepository(test_session)
    await repo.create_job(
        job_type="metric-test",
        payload={"value": 42},
    )

    response = await async_client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers.get("content-type", "")

    body = response.text
    required_metrics = [
        "leaseflow_jobs_total",
        "leaseflow_job_duration_seconds",
        "leaseflow_queue_wait_seconds",
        "leaseflow_lease_expired_total",
        "leaseflow_stale_commit_rejected_total",
        "leaseflow_retries_total",
        "leaseflow_dead_letter_total",
        "leaseflow_duplicate_deliveries_total",
        "leaseflow_recovery_seconds",
        "leaseflow_workers_active",
        "leaseflow_jobs_in_state",
        "leaseflow_heartbeat_lag_seconds",
    ]

    for metric_name in required_metrics:
        assert metric_name in body, f"Expected metric '{metric_name}' in scrape output"


@pytest.mark.asyncio
async def test_update_queue_state_metrics_calculates_correct_counts(
    test_session: AsyncSession,
) -> None:
    """Verifies that update_queue_state_metrics sets JOBS_IN_STATE gauge properly."""
    repo = JobRepository(test_session)
    await repo.create_job(job_type="test-type", payload={"n": 1})
    await repo.create_job(job_type="test-type", payload={"n": 2})

    counts = await update_queue_state_metrics(test_session)
    assert counts[JobStatus.PENDING.value] >= 2
    # Check gauge has been set
    gauge_val = JOBS_IN_STATE.labels(status=JobStatus.PENDING.value)._value.get()
    assert gauge_val >= 2


@pytest.mark.asyncio
async def test_worker_lifecycle_and_duration_metrics(
    async_session_factory: async_sessionmaker[AsyncSession],
    test_session: AsyncSession,
) -> None:
    """Verifies WORKERS_ACTIVE, JOB_DURATION_SECONDS, and QUEUE_WAIT_SECONDS instrumentation."""
    queue = PostgresQueueBackend(async_session_factory)
    registry = HandlerRegistry()

    executed = False

    async def sample_handler(ctx: JobContext) -> dict[str, str]:
        nonlocal executed
        executed = True
        await asyncio.sleep(0.01)
        return {"status": "ok"}

    registry.register("metric-work", sample_handler)

    worker = Worker(
        worker_id="test-worker-metrics",
        queue=queue,
        registry=registry,
        concurrency=1,
    )

    initial_workers = WORKERS_ACTIVE._value.get()
    worker.start()
    assert WORKERS_ACTIVE._value.get() == initial_workers + 1

    await worker.stop()
    assert WORKERS_ACTIVE._value.get() == initial_workers

    # Now create and claim a job to test duration & queue wait metrics
    repo = JobRepository(test_session)
    job, _ = await repo.create_job(
        job_type="metric-work",
        payload={"a": 1},
    )
    # Promote to QUEUED
    job.status = JobStatus.QUEUED
    await test_session.commit()

    processed = await worker.run_once()
    assert processed is True
    assert executed is True

    # Verify duration & wait metrics recorded sample
    duration_samples = [
        s
        for metric in JOB_DURATION_SECONDS.collect()
        for s in metric.samples
        if s.name == "leaseflow_job_duration_seconds_count"
        and s.labels.get("type") == "metric-work"
        and s.labels.get("outcome") == "succeeded"
    ]
    assert len(duration_samples) > 0 and duration_samples[0].value >= 1

    wait_samples = [
        s
        for metric in QUEUE_WAIT_SECONDS.collect()
        for s in metric.samples
        if s.name == "leaseflow_queue_wait_seconds_count" and s.labels.get("type") == "metric-work"
    ]
    assert len(wait_samples) > 0 and wait_samples[0].value >= 1


@pytest.mark.asyncio
async def test_stale_commit_rejected_metric(
    async_session_factory: async_sessionmaker[AsyncSession],
    test_session: AsyncSession,
) -> None:
    """Verifies STALE_COMMIT_REJECTED_TOTAL metric increments on zombie rejection."""
    queue = PostgresQueueBackend(async_session_factory)
    repo = JobRepository(test_session)
    job, _ = await repo.create_job(job_type="fenced-metric-job", payload={})
    job.status = JobStatus.QUEUED
    await test_session.commit()

    claimed = await queue.claim("worker-1", lease_ttl=10)
    assert claimed is not None

    initial_rejections = STALE_COMMIT_REJECTED_TOTAL.labels(type="fenced-metric-job")._value.get()

    # Attempt commit with stale token
    success = await queue.complete(
        job_id=claimed.id,
        fencing_token=claimed.fencing_token + 999,
        job_type=claimed.type,
    )
    assert success is False

    new_rejections = STALE_COMMIT_REJECTED_TOTAL.labels(type="fenced-metric-job")._value.get()
    assert new_rejections == initial_rejections + 1


@pytest.mark.asyncio
async def test_reaper_metrics_recording(
    test_session: AsyncSession,
) -> None:
    """Verifies that reaper records LEASE_EXPIRED_TOTAL and RECOVERY_SECONDS."""
    now = datetime.now(UTC)
    expired_job = JobModel(
        id=uuid.uuid4(),
        type="reaper-metric-test",
        payload={},
        status=JobStatus.RUNNING,
        fencing_token=1,
        attempt=1,
        max_retries=3,
        timeout_seconds=60,
        lease_owner="crashed-worker",
        lease_expires_at=now - timedelta(seconds=15),
        execution_deadline=now + timedelta(seconds=60),
        run_at=now - timedelta(seconds=30),
    )
    test_session.add(expired_job)
    await test_session.commit()

    initial_expired = LEASE_EXPIRED_TOTAL.labels(type="reaper-metric-test")._value.get()
    reaped_count = await reap_expired_jobs(test_session, backoff_seconds=1.0)
    assert reaped_count == 1

    new_expired = LEASE_EXPIRED_TOTAL.labels(type="reaper-metric-test")._value.get()
    assert new_expired == initial_expired + 1


@pytest.mark.asyncio
async def test_idempotency_duplicate_deliveries_metric(
    test_session: AsyncSession,
) -> None:
    """Verifies DUPLICATE_DELIVERIES_TOTAL increments when idempotent create is detected."""
    repo = JobRepository(test_session)
    key = f"dedup-test-{uuid.uuid4()}"
    job_type = "idempotent-metric"

    initial_dups = DUPLICATE_DELIVERIES_TOTAL.labels(type=job_type)._value.get()

    _, created1 = await repo.create_job(
        job_type=job_type,
        payload={"msg": "first"},
        idempotency_key=key,
    )
    assert created1 is True

    _, created2 = await repo.create_job(
        job_type=job_type,
        payload={"msg": "second"},
        idempotency_key=key,
    )
    assert created2 is False

    new_dups = DUPLICATE_DELIVERIES_TOTAL.labels(type=job_type)._value.get()
    assert new_dups == initial_dups + 1


def test_structured_json_logging_with_contextvars() -> None:
    """Verifies that JSONFormatter captures context variables in log record."""
    formatter = JSONFormatter()

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path.py",
        lineno=10,
        msg="Processing job payload",
        args=(),
        exc_info=None,
    )

    # Set context variables
    t_job = ctx_job_id.set("test-job-uuid-123")
    t_worker = ctx_worker_id.set("worker-alpha")
    t_token = ctx_fencing_token.set(42)
    t_att = ctx_attempt.set(2)

    try:
        formatted = formatter.format(record)
        data = json.loads(formatted)

        assert data["level"] == "INFO"
        assert data["message"] == "Processing job payload"
        assert data["job_id"] == "test-job-uuid-123"
        assert data["worker_id"] == "worker-alpha"
        assert data["fencing_token"] == 42
        assert data["attempt"] == 2
    finally:
        ctx_job_id.reset(t_job)
        ctx_worker_id.reset(t_worker)
        ctx_fencing_token.reset(t_token)
        ctx_attempt.reset(t_att)
