import json
import logging
from datetime import timedelta

import pytest
from httpx import AsyncClient

from app.core.clock import FrozenClock, SystemClock
from app.core.config import Settings
from app.core.logging import (
    JSONFormatter,
    ctx_attempt,
    ctx_fencing_token,
    ctx_job_id,
    ctx_worker_id,
)


def test_frozen_clock_advance(frozen_clock: FrozenClock) -> None:
    """Verifies that FrozenClock starts at fixed time and advances deterministically."""
    t0 = frozen_clock.now()
    m0 = frozen_clock.monotonic()

    frozen_clock.advance(15.5)

    assert frozen_clock.now() == t0 + timedelta(seconds=15.5)
    assert frozen_clock.monotonic() == m0 + 15.5


def test_system_clock() -> None:
    """Verifies that SystemClock returns valid timezone-aware UTC datetime."""
    clock = SystemClock()
    now = clock.now()
    assert now.tzinfo is not None
    assert clock.monotonic() > 0


def test_settings(test_settings: Settings) -> None:
    """Verifies that settings load correctly with custom overrides."""
    assert test_settings.ENVIRONMENT == "test"
    assert test_settings.LEASE_TTL_SECONDS == 10
    assert test_settings.HEARTBEAT_INTERVAL_SECONDS == 3


def test_structured_logging() -> None:
    """Verifies that JSONFormatter captures context variables for distributed tracing."""
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Worker heartbeat dispatched",
        args=(),
        exc_info=None,
    )

    # Set context variables
    token_job = ctx_job_id.set("job-abc-123")
    token_worker = ctx_worker_id.set("worker-node-1")
    token_fencing = ctx_fencing_token.set(42)
    token_att = ctx_attempt.set(1)

    try:
        output = formatter.format(record)
        data = json.loads(output)

        assert data["message"] == "Worker heartbeat dispatched"
        assert data["level"] == "INFO"
        assert data["job_id"] == "job-abc-123"
        assert data["worker_id"] == "worker-node-1"
        assert data["fencing_token"] == 42
        assert data["attempt"] == 1
    finally:
        ctx_job_id.reset(token_job)
        ctx_worker_id.reset(token_worker)
        ctx_fencing_token.reset(token_fencing)
        ctx_attempt.reset(token_att)


@pytest.mark.asyncio
async def test_healthz_endpoint(async_client: AsyncClient) -> None:
    """Verifies /healthz liveness endpoint."""
    response = await async_client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_endpoint(async_client: AsyncClient) -> None:
    """Verifies /readyz readiness endpoint."""
    response = await async_client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_metrics_endpoint(async_client: AsyncClient) -> None:
    """Verifies /metrics Prometheus scrape endpoint."""
    response = await async_client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
