import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.metrics import (
    DUPLICATE_DELIVERIES_TOTAL,
    JOBS_TOTAL,
    RETRIES_TOTAL,
)
from app.db.models import ExecutionModel, JobModel
from app.domain.errors import JobNotFoundError
from app.domain.state_machine import ExecutionOutcome, JobStatus, transition


class JobRepository:
    """Repository handling CRUD and persistence operations for Jobs and Executions."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_job(
        self,
        job_type: str,
        payload: dict[str, Any],
        priority: int = 0,
        run_at: datetime | None = None,
        idempotency_key: str | None = None,
        max_retries: int = 3,
        timeout_seconds: int = 60,
    ) -> tuple[JobModel, bool]:
        """
        Creates a new job with idempotency support.
        Returns: (job, created)
          - If new job created: (job, True)
          - If job with same (type, idempotency_key) exists: (existing_job, False)
        """
        if run_at is None:
            run_at = datetime.now(UTC)

        # Idempotency check: if key is present, check existing first
        if idempotency_key:
            stmt = select(JobModel).where(
                JobModel.type == job_type,
                JobModel.idempotency_key == idempotency_key,
            )
            existing = await self.session.scalar(stmt)
            if existing:
                DUPLICATE_DELIVERIES_TOTAL.labels(type=job_type).inc()
                return existing, False

        job = JobModel(
            id=uuid.uuid4(),
            type=job_type,
            payload=payload,
            status=JobStatus.PENDING,
            priority=priority,
            run_at=run_at,
            idempotency_key=idempotency_key,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            attempt=0,
            fencing_token=0,
        )

        self.session.add(job)
        try:
            await self.session.commit()
            await self.session.refresh(job)
            JOBS_TOTAL.labels(type=job_type, status=JobStatus.PENDING.value).inc()
            return job, True
        except IntegrityError:
            # Race condition: another worker inserted same (type, idempotency_key) simultaneously
            await self.session.rollback()
            if idempotency_key:
                stmt = select(JobModel).where(
                    JobModel.type == job_type,
                    JobModel.idempotency_key == idempotency_key,
                )
                existing = await self.session.scalar(stmt)
                if existing:
                    DUPLICATE_DELIVERIES_TOTAL.labels(type=job_type).inc()
                    return existing, False
            raise

    async def get_by_id(self, job_id: uuid.UUID, load_executions: bool = False) -> JobModel | None:
        """Retrieves a job by its UUID, optionally preloading executions."""
        if load_executions:
            stmt = (
                select(JobModel)
                .where(JobModel.id == job_id)
                .options(selectinload(JobModel.executions))
            )
            return await self.session.scalar(stmt)
        return await self.session.get(JobModel, job_id)

    async def cancel_job(self, job_id: uuid.UUID) -> JobModel:
        """
        Cancels a job if it is in an allowed non-terminal state.
        Raises JobNotFoundError or InvalidStateTransitionError.
        """
        job = await self.session.get(JobModel, job_id)
        if not job:
            raise JobNotFoundError(str(job_id))

        now = datetime.now(UTC)
        # Enforce state transition rules (ADR-6)
        job.status = transition(job.status, JobStatus.CANCELLED)
        job.lease_owner = None
        job.lease_expires_at = None
        job.updated_at = now

        # If an execution was open, close it with CANCELLED
        stmt = select(ExecutionModel).where(
            ExecutionModel.job_id == job_id,
            ExecutionModel.finished_at.is_(None),
        )
        open_exec = await self.session.scalar(stmt)
        if open_exec:
            open_exec.finished_at = now
            open_exec.outcome = ExecutionOutcome.CANCELLED.value

        await self.session.commit()
        await self.session.refresh(job)
        JOBS_TOTAL.labels(type=job.type, status=JobStatus.CANCELLED.value).inc()
        return job

    async def get_executions(self, job_id: uuid.UUID) -> list[ExecutionModel]:
        """Returns the execution attempt history for a specific job."""
        stmt = (
            select(ExecutionModel)
            .where(ExecutionModel.job_id == job_id)
            .order_by(ExecutionModel.attempt.asc())
        )
        result = await self.session.scalars(stmt)
        return list(result.all())

    async def retry_job(self, job_id: uuid.UUID) -> JobModel:
        """
        Manually resurrects a job from DEAD_LETTER to QUEUED (ADR-6).
        Resets attempt counter and sets run_at to now.
        Raises JobNotFoundError or InvalidStateTransitionError.
        """
        job = await self.session.get(JobModel, job_id)
        if not job:
            raise JobNotFoundError(str(job_id))

        now = datetime.now(UTC)
        job.status = transition(job.status, JobStatus.QUEUED)
        job.lease_owner = None
        job.lease_expires_at = None
        job.execution_deadline = None
        job.run_at = now
        job.attempt = 0
        job.last_error = None
        job.updated_at = now

        await self.session.commit()
        await self.session.refresh(job)
        RETRIES_TOTAL.labels(type=job.type).inc()
        JOBS_TOTAL.labels(type=job.type, status=JobStatus.QUEUED.value).inc()
        return job

    async def list_jobs(
        self,
        status: JobStatus | None = None,
        job_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[JobModel]:
        """Lists jobs with optional filtering by status and type."""
        stmt = select(JobModel)
        if status is not None:
            stmt = stmt.where(JobModel.status == status)
        if job_type is not None:
            stmt = stmt.where(JobModel.type == job_type)
        stmt = stmt.order_by(JobModel.created_at.desc()).limit(limit).offset(offset)
        result = await self.session.scalars(stmt)
        return list(result.all())
