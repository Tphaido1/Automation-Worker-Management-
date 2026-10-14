from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    DATABASE_URL: str = (
        "postgresql+asyncpg://leaseflow:leaseflow_secret@localhost:5432/leaseflow_db"
    )
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10

    # Lease & Timing (ADR-4)
    LEASE_TTL_SECONDS: int = 30
    HEARTBEAT_INTERVAL_SECONDS: int = 10  # TTL / 3
    REAPER_INTERVAL_SECONDS: int = 3
    PROMOTER_INTERVAL_SECONDS: int = 1

    # Job Defaults
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_TIMEOUT_SECONDS: int = 60

    # API Server
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    ENVIRONMENT: str = "development"


@lru_cache
def get_settings() -> Settings:
    return Settings()
