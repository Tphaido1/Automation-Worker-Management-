import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ExecutionModel, JobModel, OutboxModel
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.outbox.dispatcher import OutboxDispatcher
from app.outbox.fake_smtp import FakeSMTPServer, make_email_outbox_handler
from app.queue.postgres import PostgresQueueBackend
from app.worker.context import JobContext
from app.worker.registry import HandlerRegistry
from app.worker.worker import Worker


@pytest.mark.asyncio
async def test_atomic_commit_with_outbox_events(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Verifies that when a worker completes a job, both the JobModel state update (SUCCEEDED)
    and all buffered OutboxModel records are committed in the same atomic transaction (ADR-5).
    """
    queue = PostgresQueueBackend(async_session_factory)
    registry = HandlerRegistry()

    @registry.register("order_fulfillment")
    async def fulfill_order(ctx: JobContext) -> dict:
        # Buffer two separate outbox events
        ctx.emit_outbox(
            kind="email",
            payload={"to": "customer@example.com", "subject": "Order Shipped"},
        )
        ctx.emit_outbox(
            kind="webhook",
            payload={"event": "order.fulfilled", "order_id": ctx.payload["order_id"]},
        )
        return {"fulfilled": True}

    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="order_fulfillment",
            payload={"order_id": 9901},
            status=JobStatus.QUEUED,
            idempotency_key="order-9901-key",
            run_at=now,
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="worker-outbox-1", queue=queue, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    # Inspect database
    async with async_session_factory() as session:
        completed_job = await session.get(JobModel, job_id)
        assert completed_job is not None
        assert completed_job.status == JobStatus.SUCCEEDED
        assert completed_job.result == {"fulfilled": True}

        # Verify outbox records
        outbox_stmt = (
            select(OutboxModel).where(OutboxModel.job_id == job_id).order_by(OutboxModel.kind.asc())
        )
        outbox_records = (await session.scalars(outbox_stmt)).all()
        assert len(outbox_records) == 2

        email_rec = outbox_records[0]
        assert email_rec.kind == "email"
        assert email_rec.dedup_key == "order-9901-key:email"
        assert email_rec.payload["to"] == "customer@example.com"
        assert email_rec.dispatched_at is None

        webhook_rec = outbox_records[1]
        assert webhook_rec.kind == "webhook"
        assert webhook_rec.dedup_key == "order-9901-key:webhook"
        assert webhook_rec.payload["order_id"] == 9901


@pytest.mark.asyncio
async def test_worker_crash_before_commit_produces_zero_outbox_records(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Verifies Core Guarantee G5:
    If worker crashes / loses lease before committing, the transaction rolls back,
    resulting in ZERO outbox records committed. No side-effects are leaked.
    """
    queue = PostgresQueueBackend(async_session_factory)
    registry = HandlerRegistry()

    @registry.register("flaky_external_call")
    async def flaky_handler(ctx: JobContext) -> None:
        ctx.emit_outbox(
            kind="email",
            payload={"to": "nobody@example.com", "subject": "Should Not Exist"},
        )
        # Simulate worker crash mid-execution before commit
        raise RuntimeError("Crash simulation before DB commit")

    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="flaky_external_call",
            payload={},
            status=JobStatus.QUEUED,
            max_retries=1,
            run_at=now,
        )
        session.add(job)
        await session.commit()

    worker = Worker(worker_id="worker-crash-test", queue=queue, registry=registry)
    await worker.run_once()

    # Verify no outbox record exists
    async with async_session_factory() as session:
        outbox_stmt = select(OutboxModel).where(OutboxModel.job_id == job_id)
        outbox_records = (await session.scalars(outbox_stmt)).all()
        assert len(outbox_records) == 0


@pytest.mark.asyncio
async def test_stale_fencing_token_rejects_outbox_emission(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    Verifies that a zombie worker attempting to commit with a stale fencing token
    has its outbox events rejected and discarded (ADR-3, ADR-5).
    """
    queue = PostgresQueueBackend(async_session_factory)
    job_id = uuid.uuid4()
    now = datetime.now(UTC)

    # Job is currently at fencing_token = 2
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="payment_charge",
            payload={},
            status=JobStatus.RUNNING,
            fencing_token=2,
            attempt=2,
            run_at=now,
        )
        session.add(job)
        exec_model = ExecutionModel(
            job_id=job_id,
            attempt=1,
            worker_id="zombie-worker",
            fencing_token=1,  # Old token
            started_at=now - timedelta(seconds=60),
        )
        session.add(exec_model)
        await session.commit()

    # Zombie worker attempts to commit with stale token = 1 and outbox records
    stale_outbox = [
        {
            "job_id": job_id,
            "kind": "charge",
            "payload": {"amount": 5000},
            "dedup_key": f"{job_id}:charge",
        }
    ]
    committed = await queue.complete(
        job_id=job_id,
        fencing_token=1,  # STALE
        result={"status": "paid"},
        outbox_records=stale_outbox,
    )
    assert committed is False

    # Verify outbox table has NO records inserted
    async with async_session_factory() as session:
        outbox_stmt = select(OutboxModel).where(OutboxModel.job_id == job_id)
        records = (await session.scalars(outbox_stmt)).all()
        assert len(records) == 0

        # Verify execution record has REJECTED_STALE
        exec_stmt = select(ExecutionModel).where(
            ExecutionModel.job_id == job_id, ExecutionModel.fencing_token == 1
        )
        stale_exec = (await session.scalars(exec_stmt)).one()
        assert stale_exec.outcome == ExecutionOutcome.REJECTED_STALE.value


@pytest.mark.asyncio
async def test_end_to_end_fake_smtp_deduplication(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """
    DoD End-to-End Verification:
    1. Worker executes 'send_email' job and writes outbox event.
    2. OutboxDispatcher delivers email to FakeSMTPServer.
    3. Simulate network redelivery / re-dispatch with the exact same dedup_key.
    4. FakeSMTPServer logs duplicate attempt, but total delivered emails is EXACTLY 1.
    """
    queue = PostgresQueueBackend(async_session_factory)
    registry = HandlerRegistry()
    smtp = FakeSMTPServer()

    @registry.register("send_welcome_email")
    async def send_welcome(ctx: JobContext) -> dict:
        ctx.emit_outbox(
            kind="email",
            payload={
                "to": "newuser@example.com",
                "subject": "Welcome to LeaseFlow!",
                "body": "Your account is ready.",
            },
            dedup_key="welcome-user-42",
        )
        return {"status": "enqueued_outbox"}

    # 1. Enqueue job
    job_id = uuid.uuid4()
    now = datetime.now(UTC)
    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="send_welcome_email",
            payload={"user_id": 42},
            status=JobStatus.QUEUED,
            run_at=now,
        )
        session.add(job)
        await session.commit()

    # 2. Worker executes job
    worker = Worker(worker_id="worker-smtp", queue=queue, registry=registry)
    claimed = await worker.run_once()
    assert claimed is True

    # 3. Setup OutboxDispatcher with fake SMTP handler
    dispatcher = OutboxDispatcher(async_session_factory)
    dispatcher.register_handler("email", make_email_outbox_handler(smtp))

    # Dispatch pending outbox records
    dispatched_count = await dispatcher.dispatch_pending()
    assert dispatched_count == 1
    assert smtp.delivery_count == 1
    assert smtp.total_attempts == 1

    delivered = smtp.get_emails_for_recipient("newuser@example.com")
    assert len(delivered) == 1
    assert delivered[0].subject == "Welcome to LeaseFlow!"
    assert delivered[0].dedup_key == "welcome-user-42"

    # 4. Simulate a redelivery / re-attempt with the same dedup_key
    # (e.g. network glitch causing dispatcher to retry sending before ACK)
    duplicate_delivered = smtp.send_email(
        to="newuser@example.com",
        subject="Welcome to LeaseFlow!",
        body="Your account is ready.",
        dedup_key="welcome-user-42",
    )
    assert duplicate_delivered is False
    assert smtp.total_attempts == 2
    assert smtp.delivery_count == 1  # EXACTLY ONCE delivery preserved via dedup_key!
    assert smtp.attempts_per_dedup_key["welcome-user-42"] == 2
