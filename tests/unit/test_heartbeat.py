import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest

from app.queue.base import QueueBackend
from app.worker.context import JobContext
from app.worker.heartbeat import HeartbeatTask


@pytest.mark.asyncio
async def test_heartbeat_task_periodically_extends_lease() -> None:
    mock_queue = AsyncMock(spec=QueueBackend)
    mock_queue.heartbeat.return_value = True

    job_id = uuid.uuid4()
    context = JobContext(
        job_id=job_id,
        job_type="test",
        attempt=1,
        fencing_token=42,
        payload={},
    )

    heartbeat = HeartbeatTask(
        queue=mock_queue,
        job_id=job_id,
        fencing_token=42,
        lease_ttl=10,
        interval_seconds=0.05,
        context=context,
    )

    async with heartbeat:
        await asyncio.sleep(0.18)

    assert mock_queue.heartbeat.call_count >= 2
    mock_queue.heartbeat.assert_called_with(job_id, 42, lease_ttl=10)
    assert context.is_lease_valid is True


@pytest.mark.asyncio
async def test_heartbeat_task_handles_lease_loss() -> None:
    mock_queue = AsyncMock(spec=QueueBackend)
    mock_queue.heartbeat.return_value = False  # Simulate lease lost in DB

    job_id = uuid.uuid4()
    context = JobContext(
        job_id=job_id,
        job_type="test",
        attempt=1,
        fencing_token=5,
        payload={},
    )

    cancelled = False

    def on_lost() -> None:
        nonlocal cancelled
        cancelled = True

    heartbeat = HeartbeatTask(
        queue=mock_queue,
        job_id=job_id,
        fencing_token=5,
        lease_ttl=10,
        interval_seconds=0.05,
        context=context,
        on_lease_lost=on_lost,
    )

    async with heartbeat:
        await asyncio.sleep(0.12)

    assert mock_queue.heartbeat.call_count >= 1
    assert context.is_lease_valid is False
    assert cancelled is True
