import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import CursorResult, case, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.metrics import (
    DEAD_LETTER_TOTAL,
    JOBS_TOTAL,
    QUEUE_WAIT_SECONDS,
    RETRIES_TOTAL,
    STALE_COMMIT_REJECTED_TOTAL,
)
from app.db.models import ExecutionModel, JobModel, OutboxModel
from app.domain.state_machine import ExecutionOutcome, JobStatus
from app.queue.base import QueueBackend


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


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

            # Record claim metrics
            wait_time = max(0.0, (now - _to_utc(job.run_at)).total_seconds())
            QUEUE_WAIT_SECONDS.labels(type=job.type).observe(wait_time)
            JOBS_TOTAL.labels(type=job.type, status=JobStatus.RUNNING.value).inc()

            return job

    async def complete(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        result: dict[str, Any] | None = None,
        outbox_records: list[dict[str, Any]] | None = None,
        job_type: str = "default",
    ) -> bool:
        """
        Atomically commits job completion as SUCCEEDED and inserts outbox records (ADR-5).
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
                STALE_COMMIT_REJECTED_TOTAL.labels(type=job_type).inc()
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

            # Atomically insert outbox records within the same transaction (ADR-5)
            if outbox_records:
                for rec in outbox_records:
                    outbox_entry = OutboxModel(
                        job_id=job_id,
                        dedup_key=rec["dedup_key"],
                        kind=rec["kind"],
                        payload=rec.get("payload", {}),
                        created_at=now,
                    )
                    session.add(outbox_entry)

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
            JOBS_TOTAL.labels(type=job_type, status=JobStatus.SUCCEEDED.value).inc()
            return True

    async def fail(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        error: str,
        retryable: bool = True,
        backoff_seconds: float = 0.0,
        job_type: str = "default",
    ) -> bool:
        """
        Records job failure. Fenced by token.
        If retryable and attempt < max_retries, transitions to RETRY_WAITING with run_at = now + backoff_seconds.
        Otherwise transitions to DEAD_LETTER (ADR-5).
        """
        now = datetime.now(UTC)
        retry_run_at = now + timedelta(seconds=backoff_seconds)

        status_expr: Any
        run_at_expr: Any
        if not retryable:
            status_expr = JobStatus.DEAD_LETTER
            run_at_expr = JobModel.run_at
        else:
            status_expr = case(
                (JobModel.attempt < JobModel.max_retries, JobStatus.RETRY_WAITING),
                else_=JobStatus.DEAD_LETTER,
            )
            run_at_expr = case(
                (JobModel.attempt < JobModel.max_retries, retry_run_at),
                else_=JobModel.run_at,
            )

        async with self.session_factory() as session:
            stmt = (
                update(JobModel)
                .where(
                    JobModel.id == job_id,
                    JobModel.status == JobStatus.RUNNING,
                    JobModel.fencing_token == fencing_token,
                )
                .values(
                    status=status_expr,
                    run_at=run_at_expr,
                    last_error=error,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
            )

            res = await session.execute(stmt)
            rowcount = res.rowcount if isinstance(res, CursorResult) else 0
            if rowcount == 0:
                STALE_COMMIT_REJECTED_TOTAL.labels(type=job_type).inc()
                exec_stmt = (
                    update(ExecutionModel)
                    .where(
                        ExecutionModel.job_id == job_id,
                        ExecutionModel.fencing_token == fencing_token,
                    )
                    .values(
                        finished_at=now,
                        outcome=ExecutionOutcome.REJECTED_STALE.value,
                        error=f"Stale fencing token: rejected fail attempt ({error})",
                    )
                )
                await session.execute(exec_stmt)
                await session.commit()
                return False

            # Query updated status for accurate metric labeling
            job_status_stmt = select(JobModel.status).where(JobModel.id == job_id)
            res_status = await session.scalar(job_status_stmt)
            if res_status == JobStatus.RETRY_WAITING:
                RETRIES_TOTAL.labels(type=job_type).inc()
                JOBS_TOTAL.labels(type=job_type, status=JobStatus.RETRY_WAITING.value).inc()
            elif res_status == JobStatus.DEAD_LETTER:
                DEAD_LETTER_TOTAL.labels(type=job_type).inc()
                JOBS_TOTAL.labels(type=job_type, status=JobStatus.DEAD_LETTER.value).inc()

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

    async def heartbeat(
        self,
        job_id: uuid.UUID,
        fencing_token: int,
        lease_ttl: int = 30,
        job_type: str = "default",
    ) -> bool:
        """
        Atomically extends lease_expires_at for a RUNNING job.
        Fenced by fencing_token (ADR-3, ADR-4).
        Returns True if extended, False if rowcount == 0 (lease lost or token stale).
        """
        now = datetime.now(UTC)
        new_expiry = now + timedelta(seconds=lease_ttl)

        async with self.session_factory() as session:
            stmt = (
                update(JobModel)
                .where(
                    JobModel.id == job_id,
                    JobModel.status == JobStatus.RUNNING,
                    JobModel.fencing_token == fencing_token,
                )
                .values(
                    lease_expires_at=new_expiry,
                    updated_at=now,
                )
            )
            res = await session.execute(stmt)
            rowcount = res.rowcount if isinstance(res, CursorResult) else 0
            if rowcount > 0:
                await session.commit()
                return True
            STALE_COMMIT_REJECTED_TOTAL.labels(type=job_type).inc()
            await session.rollback()
            return False
