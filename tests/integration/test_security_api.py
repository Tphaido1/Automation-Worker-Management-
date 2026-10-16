from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.db.session import get_db_session
from app.main import app


@pytest.fixture
async def secure_client(test_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Client configured with mandatory API key authentication."""

    def override_settings() -> Settings:
        return Settings(
            ENVIRONMENT="test",
            REQUIRE_API_KEY=True,
            API_KEY="test-vault-secret-key",
            MAX_PAYLOAD_BYTES=5000,  # 5KB test limit
        )

    async def override_db() -> AsyncGenerator[AsyncSession, None]:
        yield test_session

    app.dependency_overrides[get_settings] = override_settings
    app.dependency_overrides[get_db_session] = override_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_public_system_endpoints_bypass_auth(secure_client: AsyncClient) -> None:
    """System endpoints (/healthz, /readyz, /metrics) do not require authentication."""
    resp_health = await secure_client.get("/healthz")
    assert resp_health.status_code == 200

    resp_ready = await secure_client.get("/readyz")
    assert resp_ready.status_code == 200

    resp_metrics = await secure_client.get("/metrics")
    assert resp_metrics.status_code == 200


@pytest.mark.asyncio
async def test_jobs_endpoint_requires_api_key_when_configured(secure_client: AsyncClient) -> None:
    """Protected endpoints reject requests without X-API-Key with 401."""
    # 1. Missing API Key
    resp = await secure_client.post("/jobs", json={"type": "test_job", "payload": {}})
    assert resp.status_code == 401
    assert "missing" in resp.json()["detail"].lower()

    # 2. Invalid API Key
    resp_invalid = await secure_client.post(
        "/jobs",
        json={"type": "test_job", "payload": {}},
        headers={"X-API-Key": "wrong-secret"},
    )
    assert resp_invalid.status_code == 403
    assert "invalid" in resp_invalid.json()["detail"].lower()

    # 3. Valid API Key
    resp_valid = await secure_client.post(
        "/jobs",
        json={"type": "test_job", "payload": {"k": "v"}},
        headers={"X-API-Key": "test-vault-secret-key"},
    )
    assert resp_valid.status_code == 201
    data = resp_valid.json()
    assert data["type"] == "test_job"


@pytest.mark.asyncio
async def test_jobs_endpoint_enforces_payload_size_limit(secure_client: AsyncClient) -> None:
    """Payloads exceeding MAX_PAYLOAD_BYTES return 422 Unprocessable Entity."""
    big_payload = {"large_field": "a" * (1024 * 1024 + 100)}  # > 1MB default limit
    resp = await secure_client.post(
        "/jobs",
        json={"type": "oversized_job", "payload": big_payload},
        headers={"X-API-Key": "test-vault-secret-key"},
    )
    assert resp.status_code == 422
    assert "exceeds maximum allowed limit" in resp.text


@pytest.mark.asyncio
async def test_jobs_endpoint_rejects_malicious_type_pattern(secure_client: AsyncClient) -> None:
    """Job type with invalid characters returns 422."""
    resp = await secure_client.post(
        "/jobs",
        json={"type": "malicious/type;DROP TABLE", "payload": {}},
        headers={"X-API-Key": "test-vault-secret-key"},
    )
    assert resp.status_code == 422
