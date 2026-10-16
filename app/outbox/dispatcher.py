import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxModel

logger = logging.getLogger(__name__)

OutboxHandler = Callable[[dict[str, Any], str], Awaitable[None] | None]


class OutboxDispatcher:
    """
    Asynchronous Outbox Dispatcher (ADR-5).
    Reads pending outbox records from PostgreSQL using SKIP LOCKED,
    invokes registered side-effect handlers, and marks them dispatched atomically.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self._handlers: dict[str, OutboxHandler] = {}
        self._sqlite_lock = asyncio.Lock()
        bind = session_factory.kw.get("bind") or getattr(session_factory, "bind", None)
        self._is_sqlite = bool(
            bind and getattr(bind, "dialect", None) and bind.dialect.name == "sqlite"
        )

    def register_handler(self, kind: str, handler: OutboxHandler) -> None:
        """Registers a side-effect publisher for a specific outbox event kind."""
        self._handlers[kind] = handler

    async def get_pending_count(self) -> int:
        """Returns the number of undispatched outbox records."""
        async with self.session_factory() as session:
            stmt = select(func.count(OutboxModel.id)).where(OutboxModel.dispatched_at.is_(None))
            count = await session.scalar(stmt)
            return count or 0

    async def dispatch_pending(self, limit: int = 50) -> int:
        """
        Fetches and dispatches pending outbox events.
        Uses FOR UPDATE SKIP LOCKED on PostgreSQL for safe concurrent multi-dispatcher execution.
        """
        if self._is_sqlite:
            async with self._sqlite_lock:
                return await self._dispatch_batch(limit)
        return await self._dispatch_batch(limit)

    async def _dispatch_batch(self, limit: int) -> int:
        now = datetime.now(UTC)
        dispatched_count = 0

        async with self.session_factory() as session:
            stmt = (
                select(OutboxModel)
                .where(OutboxModel.dispatched_at.is_(None))
                .order_by(OutboxModel.created_at.asc())
                .limit(limit)
            )
            if not self._is_sqlite:
                stmt = stmt.with_for_update(skip_locked=True)

            records = (await session.scalars(stmt)).all()
            if not records:
                return 0

            for record in records:
                handler = self._handlers.get(record.kind)
                if not handler:
                    logger.warning(
                        "No handler registered for outbox record kind '%s' (id=%d)",
                        record.kind,
                        record.id,
                    )
                    continue

                try:
                    if inspect.iscoroutinefunction(handler):
                        await handler(record.payload, record.dedup_key)
                    else:
                        handler(record.payload, record.dedup_key)

                    record.dispatched_at = now
                    dispatched_count += 1
                    logger.info(
                        "Dispatched outbox event id=%d, kind='%s', dedup_key='%s'",
                        record.id,
                        record.kind,
                        record.dedup_key,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to dispatch outbox event id=%d (dedup_key='%s'): %s",
                        record.id,
                        record.dedup_key,
                        e,
                        exc_info=True,
                    )

            if dispatched_count > 0:
                await session.commit()
            else:
                await session.rollback()

        return dispatched_count


class OutboxDispatcherLoop:
    """Background service that periodically polls and dispatches outbox events."""

    def __init__(
        self,
        dispatcher: OutboxDispatcher,
        interval_seconds: float = 1.0,
        batch_size: int = 50,
    ) -> None:
        self.dispatcher = dispatcher
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def run_once(self) -> int:
        """Executes a single dispatch cycle."""
        return await self.dispatcher.dispatch_pending(limit=self.batch_size)

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once()
            except Exception as e:
                logger.error("Error in outbox dispatcher loop: %s", e, exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="leaseflow-outbox-dispatcher")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
