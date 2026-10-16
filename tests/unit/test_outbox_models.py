import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobModel, OutboxModel
from app.domain.state_machine import JobStatus
from app.worker.context import JobContext


def test_job_context_emit_outbox_defaults() -> None:
    """Verifies default dedup_key formatting when idempotency_key is present or absent."""
    job_id = uuid.uuid4()

    # Case 1: with idempotency_key
    ctx1 = JobContext(
        job_id=job_id,
        job_type="notify_user",
        attempt=1,
        fencing_token=1,
        payload={"msg": "hello"},
        idempotency_key="order-12345",
    )
    ctx1.emit_outbox(kind="email", payload={"to": "alice@example.com"})
    assert len(ctx1.pending_outbox_records) == 1
    assert ctx1.pending_outbox_records[0]["dedup_key"] == "order-12345:email"

    # Case 2: without idempotency_key (falls back to job_id)
    ctx2 = JobContext(
        job_id=job_id,
        job_type="notify_user",
        attempt=1,
        fencing_token=1,
        payload={"msg": "hello"},
    )
    ctx2.emit_outbox(kind="webhook", payload={"url": "https://example.com/hook"})
    assert len(ctx2.pending_outbox_records) == 1
    assert ctx2.pending_outbox_records[0]["dedup_key"] == f"{job_id}:webhook"

    # Case 3: explicit custom dedup_key
    ctx2.emit_outbox(
        kind="sms",
        payload={"phone": "123456789"},
        dedup_key="custom-dedup-999",
    )
    assert len(ctx2.pending_outbox_records) == 2
    assert ctx2.pending_outbox_records[1]["dedup_key"] == "custom-dedup-999"


@pytest.mark.asyncio
async def test_outbox_model_dedup_key_uniqueness(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Verifies database-level uniqueness constraint on outbox.dedup_key."""
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="send_newsletter",
            payload={},
            status=JobStatus.QUEUED,
            run_at=now,
        )
        session.add(job)
        await session.commit()

    # Insert first outbox record with key 'newsletter-2026-user1'
    async with async_session_factory() as session:
        outbox_1 = OutboxModel(
            job_id=job_id,
            dedup_key="newsletter-2026-user1",
            kind="email",
            payload={"to": "user1@example.com"},
            created_at=now,
        )
        session.add(outbox_1)
        await session.commit()

    # Attempt to insert second outbox record with duplicate key
    async with async_session_factory() as session:
        outbox_duplicate = OutboxModel(
            job_id=job_id,
            dedup_key="newsletter-2026-user1",
            kind="email",
            payload={"to": "user1@example.com"},
            created_at=now,
        )
        session.add(outbox_duplicate)
        with pytest.raises(IntegrityError):
            await session.commit()
