import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.domain.state_machine import JobStatus


class JobCreateRequest(BaseModel):
    """Request payload for scheduling a new job."""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(
        ..., description="Job type identifier for worker routing", min_length=1, max_length=255
    )
    payload: dict[str, Any] = Field(
        default_factory=dict, description="Job parameters and input data"
    )
    priority: int = Field(default=0, description="Priority integer (higher numbers claimed first)")
    run_at: datetime | None = Field(default=None, description="Scheduled execution timestamp (UTC)")
    idempotency_key: str | None = Field(
        default=None, description="Optional deduplication key", max_length=255
    )
    max_retries: int = Field(
        default=3, description="Maximum retry attempts on failure", ge=0, le=100
    )
    timeout_seconds: int = Field(
        default=60, description="Hard execution deadline in seconds", ge=1, le=86400
    )


class JobResponse(BaseModel):
    """Full representation of a job."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    run_at: datetime
    idempotency_key: str | None = None
    max_retries: int
    timeout_seconds: int
    attempt: int
    fencing_token: int
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    execution_deadline: datetime | None = None
    result: dict[str, Any] | None = None
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime


class ExecutionResponse(BaseModel):
    """Representation of an execution attempt."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    job_id: uuid.UUID
    attempt: int
    worker_id: str
    fencing_token: int
    started_at: datetime
    finished_at: datetime | None = None
    outcome: str | None = None
    error: str | None = None


class ErrorResponse(BaseModel):
    """Standard error response."""

    detail: str
