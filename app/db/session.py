from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings
from app.db.models import Base

settings = get_settings()

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Returns or creates the global AsyncEngine."""
    global _engine
    if _engine is None:
        kwargs: dict[str, object] = {}
        if "sqlite" not in settings.DATABASE_URL:
            kwargs = {
                "pool_size": settings.DB_POOL_SIZE,
                "max_overflow": settings.DB_MAX_OVERFLOW,
            }
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=(settings.ENVIRONMENT == "development"),
            **kwargs,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Returns or creates the async session factory."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency yielding an async database session with automatic rollback on error."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def init_tables(engine: AsyncEngine) -> None:
    """Creates all database tables defined in Base metadata."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_tables(engine: AsyncEngine) -> None:
    """Drops all database tables defined in Base metadata."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
