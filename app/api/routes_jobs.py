import uuid

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import ErrorResponse, ExecutionResponse, JobCreateRequest, JobResponse
from app.api.security import verify_api_key
from app.db.repositories import JobRepository
from app.db.session import get_db_session
from app.domain.errors import InvalidStateTransitionError, JobNotFoundError
from app.domain.state_machine import JobStatus

router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {
            "description": "Idempotent duplicate: existing job returned without re-creation",
            "model": JobResponse,
        },
        201: {"description": "Job successfully scheduled", "model": JobResponse},
    },
)
async def create_job(
    req: JobCreateRequest,
    response: Response,
    session: AsyncSession = Depends(get_db_session),
) -> JobResponse:
    """
    Schedules a new job. Supports idempotency key:
    If a job with the same (type, idempotency_key) already exists, returns HTTP 200 with the existing job.
    """
    repo = JobRepository(session)
    job, created = await repo.create_job(
        job_type=req.type,
        payload=req.payload,
        priority=req.priority,
        run_at=req.run_at,
        idempotency_key=req.idempotency_key,
        max_retries=req.max_retries,
        timeout_seconds=req.timeout_seconds,
    )

    if not created:
        response.status_code = status.HTTP_200_OK

    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=list[JobResponse],
    summary="List scheduled/processed jobs",
)
async def list_jobs(
    status: JobStatus | None = None,
    type: str | None = None,
    limit: int = 50,
    offset: int = 0,
    session: AsyncSession = Depends(get_db_session),
) -> list[JobResponse]:
    """Lists jobs with optional filtering by status and type."""
    repo = JobRepository(session)
    jobs = await repo.list_jobs(status=status, job_type=type, limit=limit, offset=offset)
    return [JobResponse.model_validate(j) for j in jobs]


@router.get(
    "/dead-letters",
    response_model=list[JobResponse],
    summary="List dead-lettered jobs (DLQ)",
)
async def get_dead_letters(
    limit: int = 50,
    offset: int = 0,
    session: AsyncSession = Depends(get_db_session),
) -> list[JobResponse]:
    """Lists jobs currently in DEAD_LETTER state."""
    repo = JobRepository(session)
    jobs = await repo.list_jobs(status=JobStatus.DEAD_LETTER, limit=limit, offset=offset)
    return [JobResponse.model_validate(j) for j in jobs]


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    responses={404: {"model": ErrorResponse, "description": "Job not found"}},
)
async def get_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobResponse:
    """Retrieves full details and current status of a job."""
    repo = JobRepository(session)
    job = await repo.get_by_id(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID '{job_id}' not found",
        )
    return JobResponse.model_validate(job)


@router.post(
    "/{job_id}/cancel",
    response_model=JobResponse,
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Job cannot be cancelled (e.g. already in terminal state)",
        },
        404: {"model": ErrorResponse, "description": "Job not found"},
    },
)
async def cancel_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobResponse:
    """
    Cancels an active or pending job.
    Only valid if job is not in a terminal state (SUCCEEDED, CANCELLED).
    """
    repo = JobRepository(session)
    try:
        cancelled_job = await repo.cancel_job(job_id)
        return JobResponse.model_validate(cancelled_job)
    except JobNotFoundError as err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(err),
        ) from err
    except InvalidStateTransitionError as err:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(err),
        ) from err


@router.post(
    "/{job_id}/retry",
    response_model=JobResponse,
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Job cannot be retried (e.g. not in DEAD_LETTER state)",
        },
        404: {"model": ErrorResponse, "description": "Job not found"},
    },
)
async def retry_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobResponse:
    """
    Manually retries a job stuck in DEAD_LETTER (ADR-6).
    Transitions DEAD_LETTER -> QUEUED and resets the attempt counter.
    """
    repo = JobRepository(session)
    try:
        retried_job = await repo.retry_job(job_id)
        return JobResponse.model_validate(retried_job)
    except JobNotFoundError as err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(err),
        ) from err
    except InvalidStateTransitionError as err:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(err),
        ) from err


@router.get(
    "/{job_id}/executions",
    response_model=list[ExecutionResponse],
    responses={404: {"model": ErrorResponse, "description": "Job not found"}},
)
async def get_job_executions(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> list[ExecutionResponse]:
    """Retrieves all execution attempts for a given job."""
    repo = JobRepository(session)
    job = await repo.get_by_id(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID '{job_id}' not found",
        )
    executions = await repo.get_executions(job_id)
    return [ExecutionResponse.model_validate(e) for e in executions]
