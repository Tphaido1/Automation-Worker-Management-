from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.core.config import get_settings
from app.core.logging import setup_logging

settings = get_settings()
setup_logging(level=settings.ENVIRONMENT == "production" and 20 or 10)

app = FastAPI(
    title="LeaseFlow API",
    description="Fault-tolerant Distributed Job Scheduler in Python",
    version="0.1.0",
)


@app.get("/healthz", tags=["System"])
async def healthz() -> dict[str, str]:
    """Liveness probe: verifies process is alive."""
    return {"status": "ok"}


@app.get("/readyz", tags=["System"])
async def readyz() -> dict[str, str]:
    """Readiness probe: verifies service is ready to accept traffic."""
    return {"status": "ready"}


@app.get("/metrics", tags=["System"])
async def metrics() -> Response:
    """Prometheus metrics scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
