from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.clock import FrozenClock
from app.core.config import Settings
from app.main import app


@pytest.fixture
def frozen_clock() -> FrozenClock:
    """Fixture providing a deterministic frozen clock starting at 2026-01-01 00:00:00 UTC."""
    return FrozenClock(
        initial_time=datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        initial_monotonic=1000.0,
    )


@pytest.fixture
def test_settings() -> Settings:
    """Fixture providing test configuration."""
    return Settings(
        ENVIRONMENT="test",
        LEASE_TTL_SECONDS=10,
        HEARTBEAT_INTERVAL_SECONDS=3,
        REAPER_INTERVAL_SECONDS=1,
    )


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Fixture providing an async HTTP client for FastAPI app testing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
