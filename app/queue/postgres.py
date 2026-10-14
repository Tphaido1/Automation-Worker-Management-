import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import CursorResult, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ExecutionModel, JobModel
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.queue.base import QueueBackend


class PostgresQueueBackend(QueueBackend):
    """
    PostgreSQL-based queue backend implementing atomic claim with SKIP LOCKED
    and token-fenced commits/failures (ADR-1, ADR-3).
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory
        self._sqlite_lock = asyncio.Lock()
        bind = session_factory.kw.get("bind") or getattr(session_factory, "bind", None)
        self._is_sqlite = bool(
            bind and getattr(bind, "dialect", None) and bind.dialect.name == "sqlite"
        )

    async def claim(self, worker_id: str, lease_ttl: int = 30) -> JobModel | None:
        """
        Atomically claims the highest priority ready job.
        Uses SELECT ... FOR UPDATE SKIP LOCKED on PostgreSQL.
        On SQLite in-memory (tests), uses an async lock to prevent lock contention.
        Increments fencing_token, sets lease, and inserts execution record.
        """
        if self._is_sqlite:
            async with self._sqlite_lock:
                return await self._claim_tx(worker_id, lease_ttl)
        return await self._claim_tx(worker_id, lease_ttl)

    async def _claim_tx(self, worker_id: str, lease_ttl: int = 30) -> JobModel | None:
        now = datetime.now(UTC)

        async with self.session_factory() as session:
            # 1. Atomic candidate selection with SKIP LOCKED
            stmt = (
                select(JobModel)
                .where(
                    JobModel.status == JobStatus.QUEUED,
                    JobModel.run_at <= now,
                )
                .order_by(JobModel.priority.desc(), JobModel.run_at.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
            )

            job = await session.scalar(stmt)
            if not job:
                return None

            # 2. Update job lease and monotonic fencing token
            job.status = JobStatus.RUNNING
            job.lease_owner = worker_id
            job.fencing_token += 1
            job.attempt += 1
            job.lease_expires_at = now + timedelta(seconds=lease_ttl)
            job.execution_deadline = now + timedelta(seconds=job.timeout_seconds)
            job.updated_at = now

            # 3. Create execution attempt record in the same atomic transaction
            execution = ExecutionModel(
                job_id=job.id,
                attempt=job.attempt,
                worker_id=worker_id,
                fencing_token=job.fencing_token,
                started_at=now,
            )
            session.add(execution)

            await session.commit()
            await session.refresh(job)
            return job

    async def complete(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        result: dict[str, Any] | None = None,
    ) -> bool:
        """
        Atomically commits job completion as SUCCEEDED.
        Rejects commit if fencing token is stale (rowcount == 0).
        """
        now = datetime.now(UTC)

        async with self.session_factory() as session:
            stmt = (
                update(JobModel)
                .where(
                    JobModel.id == job_id,
                    JobModel.status == JobStatus.RUNNING,
                    JobModel.fencing_token == fencing_token,
                )
                .values(
                    status=JobStatus.SUCCEEDED,
                    result=result,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
            )

            res = await session.execute(stmt)
            rowcount = res.rowcount if isinstance(res, CursorResult) else 0
            if rowcount == 0:
                # Fencing token rejected: record REJECTED_STALE
                exec_stmt = (
                    update(ExecutionModel)
                    .where(
                        ExecutionModel.job_id == job_id,
                        ExecutionModel.fencing_token == fencing_token,
                    )
                    .values(
                        finished_at=now,
                        outcome=ExecutionOutcome.REJECTED_STALE.value,
                        error="Stale fencing token: rejected commit",
                    )
                )
                await session.execute(exec_stmt)
                await session.commit()
                return False

            # Mark execution successful
            exec_stmt = (
                update(ExecutionModel)
                .where(
                    ExecutionModel.job_id == job_id,
                    ExecutionModel.fencing_token == fencing_token,
                )
                .values(
                    finished_at=now,
                    outcome=ExecutionOutcome.SUCCEEDED.value,
                )
            )
            await session.execute(exec_stmt)
            await session.commit()
            return True

    async def fail(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        error: str,
    ) -> bool:
        """
        Records job failure. Fenced by token.
        """
        now = datetime.now(UTC)

        async with self.session_factory() as session:
            stmt = (
                update(JobModel)
                .where(
                    JobModel.id == job_id,
                    JobModel.status == JobStatus.RUNNING,
                    JobModel.fencing_token == fencing_token,
                )
                .values(
                    status=JobStatus.DEAD_LETTER,
                    last_error=error,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
            )

            res = await session.execute(stmt)
            rowcount = res.rowcount if isinstance(res, CursorResult) else 0
            if rowcount == 0:
                return False

            exec_stmt = (
                update(ExecutionModel)
                .where(
                    ExecutionModel.job_id == job_id,
                    ExecutionModel.fencing_token == fencing_token,
                )
                .values(
                    finished_at=now,
                    outcome=ExecutionOutcome.FAILED.value,
                    error=error,
                )
            )
            await session.execute(exec_stmt)
            await session.commit()
            return True
