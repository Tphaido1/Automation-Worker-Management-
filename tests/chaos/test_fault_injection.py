import asyncio
import logging
import time

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
)

from app.core.metrics import HEARTBEAT_LAG_SECONDS
from app.db.repositories import JobRepository
from app.domain.state_machine import JobStatus
from app.queue.postgres import PostgresQueueBackend
from app.worker.context import JobContext
from app.worker.heartbeat import HeartbeatTask
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker
from tests.chaos.fault_injection import FaultInjectionBackend

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_network_partition_during_heartbeat_invalidates_lease(
    async_session_factory: async_sessionmaker[AsyncSession],
    test_session: AsyncSession,
) -> None:
    """Verifies that network partition during heartbeat safely invalidates lease and aborts handler."""
    real_backend = PostgresQueueBackend(async_session_factory)
    faulty_backend = FaultInjectionBackend(real_backend)

    repo = JobRepository(test_session)
    job, _ = await repo.create_job(job_type="partition-test", payload={})
    job.status = JobStatus.QUEUED
    await test_session.commit()

    claimed = await faulty_backend.claim("worker-network-test", lease_ttl=10)
    assert claimed is not None

    context = JobContext(
        job_id=claimed.id,
        job_type=claimed.type,
        attempt=claimed.attempt,
        fencing_token=claimed.fencing_token,
        payload=claimed.payload,
    )

    cancelled = False

    def on_lost() -> None:
        nonlocal cancelled
        cancelled = True

    heartbeat = HeartbeatTask(
        queue=faulty_backend,
        job_id=claimed.id,
        fencing_token=claimed.fencing_token,
        lease_ttl=1,  # Short lease
        interval_seconds=0.03,
        context=context,
        on_lease_lost=on_lost,
    )
    # Set explicit small local lease deadline for test
    heartbeat._lease_deadline = time.monotonic() + 0.1

    # 1. Trigger network partition
    faulty_backend.set_network_partition(True)

    async with heartbeat:
        await asyncio.sleep(0.2)

    assert context.is_lease_valid is False
    assert cancelled is True


@pytest.mark.asyncio
async def test_transient_db_failure_in_worker_loop_does_not_crash(
    async_session_factory: async_sessionmaker[AsyncSession],
    test_session: AsyncSession,
) -> None:
    """Verifies worker loop survives transient database connection drops."""
    real_backend = PostgresQueueBackend(async_session_factory)
    faulty_backend = FaultInjectionBackend(real_backend)

    registry = HandlerRegistry()
    executed = False

    async def sample_handler(ctx: JobContext) -> dict[str, str]:
        nonlocal executed
        executed = True
        return {"status": "ok"}

    registry.register("transient-test", sample_handler)

    worker = Worker(
        worker_id="transient-worker",
        queue=faulty_backend,
        registry=registry,
        poll_interval=0.05,
    )

    # 1. Inject transient failure on the first claim() attempt
    faulty_backend.inject_failure("claim", ConnectionError("Transient DB connection reset"))

    repo = JobRepository(test_session)
    job, _ = await repo.create_job(job_type="transient-test", payload={})
    job.status = JobStatus.QUEUED
    await test_session.commit()

    worker.start()

    # Worker encounters failure on 1st loop, retries on 2nd loop and succeeds
    for _ in range(30):
        await asyncio.sleep(0.1)
        if executed:
            break

    await worker.stop()
    assert executed is True, "Worker should recover from transient failure and execute job"


@pytest.mark.asyncio
async def test_high_latency_heartbeat_records_lag_metric(
    async_session_factory: async_sessionmaker[AsyncSession],
    test_session: AsyncSession,
) -> None:
    """Verifies that slow database responses during heartbeat are recorded in HEARTBEAT_LAG_SECONDS."""
    real_backend = PostgresQueueBackend(async_session_factory)
    faulty_backend = FaultInjectionBackend(real_backend)

    repo = JobRepository(test_session)
    job, _ = await repo.create_job(job_type="lag-test", payload={})
    job.status = JobStatus.QUEUED
    await test_session.commit()

    claimed = await faulty_backend.claim("lag-worker", lease_ttl=10)
    assert claimed is not None

    context = JobContext(
        job_id=claimed.id,
        job_type=claimed.type,
        attempt=claimed.attempt,
        fencing_token=claimed.fencing_token,
        payload=claimed.payload,
    )

    heartbeat = HeartbeatTask(
        queue=faulty_backend,
        job_id=claimed.id,
        fencing_token=claimed.fencing_token,
        lease_ttl=10,
        interval_seconds=0.05,
        context=context,
    )

    # Inject 0.08s artificial delay in heartbeat execution
    faulty_backend.set_latency("heartbeat", 0.08)

    async with heartbeat:
        await asyncio.sleep(0.2)

    # Collect lag histogram samples
    samples = [
        s
        for m in HEARTBEAT_LAG_SECONDS.collect()
        for s in m.samples
        if s.name == "leaseflow_heartbeat_lag_seconds_count"
    ]
    assert len(samples) > 0 and samples[0].value >= 1
