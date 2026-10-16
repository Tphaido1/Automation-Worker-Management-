import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import CursorResult, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobModel
from app.domain.state_machine import JobStatus

logger = logging.getLogger(__name__)


async def promote_due_jobs(session: AsyncSession) -> int:
    """
    Promotes jobs whose scheduled time has arrived:
    1. PENDING -> QUEUED (run_at <= now)
    2. RETRY_WAITING -> QUEUED (run_at <= now, backoff expired)
    Returns total count of promoted jobs.
    """
    now = datetime.now(UTC)

    # 1. PENDING -> QUEUED
    stmt_pending = (
        update(JobModel)
        .where(
            JobModel.status == JobStatus.PENDING,
            JobModel.run_at <= now,
        )
        .values(
            status=JobStatus.QUEUED,
            updated_at=now,
        )
        .execution_options(synchronize_session="fetch")
    )
    res_pending = await session.execute(stmt_pending)
    count_pending = res_pending.rowcount if isinstance(res_pending, CursorResult) else 0

    # 2. RETRY_WAITING -> QUEUED
    stmt_retry = (
        update(JobModel)
        .where(
            JobModel.status == JobStatus.RETRY_WAITING,
            JobModel.run_at <= now,
        )
        .values(
            status=JobStatus.QUEUED,
            updated_at=now,
        )
        .execution_options(synchronize_session="fetch")
    )
    res_retry = await session.execute(stmt_retry)
    count_retry = res_retry.rowcount if isinstance(res_retry, CursorResult) else 0

    total_promoted = int(count_pending + count_retry)
    if total_promoted > 0:
        await session.commit()
        logger.info("Promoted %d jobs to QUEUED", total_promoted)
    else:
        await session.rollback()

    return total_promoted


class PromoterLoop:
    """Background service periodically running promote_due_jobs."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        interval_seconds: float = 1.0,
    ) -> None:
        self.session_factory = session_factory
        self.interval_seconds = interval_seconds
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> int:
        """Runs a single promotion cycle."""
        async with self.session_factory() as session:
            return await promote_due_jobs(session)

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Error in promoter loop: %s", e, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="leaseflow-promoter")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
