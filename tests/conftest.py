from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.core.clock import FrozenClock
from app.core.config import Settings
from app.db.models import Base
from app.db.session import get_db_session
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
async def test_engine() -> AsyncGenerator[AsyncEngine, None]:
    """Isolated async SQLite engine for in-memory testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
def async_session_factory(test_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Provides a sessionmaker bound to the test engine."""
    return async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


@pytest.fixture
async def test_session(test_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Provides an isolated database session per test with rollback cleanup."""
    factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    async with factory() as session:
        yield session


@pytest.fixture
async def async_client(test_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Provides an async HTTP client for FastAPI with overridden database dependency."""

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        yield test_session

    app.dependency_overrides[get_db_session] = override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()
