import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.domain.state_machine import JobStatus


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy 2.0 models."""

    pass


class JobModel(Base):
    """
    SQLAlchemy model representing a scheduled/distributed job.
    Matches schema definition in specification 3.2.
    """

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[JobStatus] = mapped_column(
        SQLEnum(JobStatus, native_enum=False, length=50),
        nullable=False,
        default=JobStatus.PENDING,
        index=True,
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Lease and fencing state (ADR-3, ADR-4)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    execution_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Result and diagnostic metadata
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationship to execution history (append-only)
    executions: Mapped[list["ExecutionModel"]] = relationship(
        "ExecutionModel",
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="ExecutionModel.attempt",
    )

    __table_args__ = (
        UniqueConstraint("type", "idempotency_key", name="uq_jobs_type_idempotency_key"),
        Index("jobs_claim_idx", "priority", "run_at"),
        Index("jobs_lease_idx", "lease_expires_at"),
        Index("jobs_retry_idx", "run_at"),
    )


class ExecutionModel(Base):
    """
    Append-only record of each execution attempt.
    Tracks worker identity, fencing token, timing, and outcome.
    """

    __tablename__ = "executions"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str | None] = mapped_column(String(50), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["JobModel"] = relationship("JobModel", back_populates="executions")


class OutboxModel(Base):
    """
    Transactional Outbox record (ADR-5).
    Guarantees atomic emission of external side-effects alongside job completion.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    dedup_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    kind: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    job: Mapped["JobModel"] = relationship("JobModel")

    __table_args__ = (Index("outbox_pending_idx", "created_at"),)
