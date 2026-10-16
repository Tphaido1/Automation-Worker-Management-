import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobModel, OutboxModel
from app.domain.state_machine import JobStatus
from app.outbox.dispatcher import OutboxDispatcher, OutboxDispatcherLoop


@pytest.mark.asyncio
async def test_outbox_dispatcher_processes_pending_events(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Verifies that OutboxDispatcher dispatches pending records and updates dispatched_at."""
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="billing_task",
            payload={},
            status=JobStatus.SUCCEEDED,
            run_at=now,
        )
        session.add(job)
        await session.commit()

    # Seed 2 outbox records
    async with async_session_factory() as session:
        o1 = OutboxModel(
            job_id=job_id,
            dedup_key=f"{job_id}:invoice_email",
            kind="email",
            payload={"to": "client@example.com", "invoice_id": "INV-100"},
            created_at=now,
        )
        o2 = OutboxModel(
            job_id=job_id,
            dedup_key=f"{job_id}:webhook",
            kind="webhook",
            payload={"event": "invoice.generated"},
            created_at=now,
        )
        session.add_all([o1, o2])
        await session.commit()

    dispatcher = OutboxDispatcher(async_session_factory)
    assert await dispatcher.get_pending_count() == 2

    dispatched_emails = []
    dispatched_webhooks = []

    async def email_handler(payload: dict, dedup_key: str) -> None:
        dispatched_emails.append((payload, dedup_key))

    async def webhook_handler(payload: dict, dedup_key: str) -> None:
        dispatched_webhooks.append((payload, dedup_key))

    dispatcher.register_handler("email", email_handler)
    dispatcher.register_handler("webhook", webhook_handler)

    # Dispatch
    count = await dispatcher.dispatch_pending()
    assert count == 2
    assert len(dispatched_emails) == 1
    assert len(dispatched_webhooks) == 1
    assert dispatched_emails[0][0]["invoice_id"] == "INV-100"

    # Verify database state
    assert await dispatcher.get_pending_count() == 0

    # Second dispatch run processes 0
    count_2 = await dispatcher.dispatch_pending()
    assert count_2 == 0


@pytest.mark.asyncio
async def test_outbox_dispatcher_handles_failing_handler(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Verifies that if a side-effect handler throws an exception, record remains pending."""
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with async_session_factory() as session:
        job = JobModel(
            id=job_id,
            type="notify",
            payload={},
            status=JobStatus.SUCCEEDED,
            run_at=now,
        )
        session.add(job)
        o = OutboxModel(
            job_id=job_id,
            dedup_key=f"{job_id}:failing_hook",
            kind="failing_hook",
            payload={"data": "val"},
            created_at=now,
        )
        session.add(o)
        await session.commit()

    dispatcher = OutboxDispatcher(async_session_factory)

    async def broken_handler(payload: dict, dedup_key: str) -> None:
        raise ConnectionRefusedError("Third party API is down")

    dispatcher.register_handler("failing_hook", broken_handler)

    count = await dispatcher.dispatch_pending()
    assert count == 0
    # Record remains pending for next retry
    assert await dispatcher.get_pending_count() == 1


@pytest.mark.asyncio
async def test_outbox_dispatcher_loop_lifecycle(
    async_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Verifies OutboxDispatcherLoop start and graceful stop."""
    dispatcher = OutboxDispatcher(async_session_factory)
    loop = OutboxDispatcherLoop(dispatcher, interval_seconds=0.05)

    loop.start()
    assert loop._running is True
    assert loop._task is not None

    await loop.stop()
    assert loop._running is False
    assert loop._task is None
