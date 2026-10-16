from fastapi import Depends, FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.routes_jobs import router as jobs_router
from app.api.schemas import JobResponse
from app.api.security import verify_api_key
from app.core.config import get_settings
from app.core.logging import setup_logging
from app.core.metrics import update_queue_state_metrics
from app.db.repositories import JobRepository
from app.db.session import get_db_session
from app.domain.state_machine import JobStatus

settings = get_settings()
setup_logging(level=settings.ENVIRONMENT == "production" and 20 or 10)

app = FastAPI(
    title="LeaseFlow API",
    description="Fault-tolerant Distributed Job Scheduler in Python",
    version="0.1.0",
)

app.include_router(jobs_router)


@app.get(
    "/dead-letters",
    response_model=list[JobResponse],
    tags=["Dead Letter Queue"],
    summary="List dead-lettered jobs (DLQ root alias)",
    dependencies=[Depends(verify_api_key)],
)
async def list_dead_letters_root(
    limit: int = 50,
    offset: int = 0,
    session: AsyncSession = Depends(get_db_session),
) -> list[JobResponse]:
    """Retrieves all jobs currently stranded in the Dead-Letter Queue."""
    repo = JobRepository(session)
    jobs = await repo.list_jobs(status=JobStatus.DEAD_LETTER, limit=limit, offset=offset)
    return [JobResponse.model_validate(j) for j in jobs]


@app.get("/healthz", tags=["System"])
async def healthz() -> dict[str, str]:
    """Liveness probe: verifies process is alive."""
    return {"status": "ok"}


@app.get("/readyz", tags=["System"])
async def readyz() -> dict[str, str]:
    """Readiness probe: verifies service is ready to accept traffic."""
    return {"status": "ready"}


@app.get("/metrics", tags=["System"])
async def metrics(session: AsyncSession = Depends(get_db_session)) -> Response:
    """Prometheus metrics scrape endpoint."""
    await update_queue_state_metrics(session)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
