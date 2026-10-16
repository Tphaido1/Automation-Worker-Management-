import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.api.schemas import JobCreateRequest
from app.api.security import verify_api_key
from app.core.config import Settings


@pytest.mark.asyncio
async def test_verify_api_key_permissive_when_unconfigured() -> None:
    """When REQUIRE_API_KEY=False and API_KEY=None, any request is allowed."""
    settings = Settings(REQUIRE_API_KEY=False, API_KEY=None)
    result = await verify_api_key(api_key=None, settings=settings)
    assert result is None

    result2 = await verify_api_key(api_key="any-random-key", settings=settings)
    assert result2 == "any-random-key"


@pytest.mark.asyncio
async def test_verify_api_key_rejects_missing_header_when_required() -> None:
    """When API_KEY is set, missing X-API-Key header raises 401 Unauthorized."""
    settings = Settings(REQUIRE_API_KEY=True, API_KEY="my-secret-token")
    with pytest.raises(HTTPException) as exc_info:
        await verify_api_key(api_key=None, settings=settings)
    assert exc_info.value.status_code == 401
    assert "missing" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_verify_api_key_rejects_invalid_token() -> None:
    """When API_KEY is set, incorrect X-API-Key header raises 403 Forbidden."""
    settings = Settings(REQUIRE_API_KEY=True, API_KEY="my-secret-token")
    with pytest.raises(HTTPException) as exc_info:
        await verify_api_key(api_key="wrong-token", settings=settings)
    assert exc_info.value.status_code == 403
    assert "invalid" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_verify_api_key_accepts_valid_token() -> None:
    """When API_KEY is set and matches, request is granted."""
    settings = Settings(REQUIRE_API_KEY=True, API_KEY="my-secret-token")
    result = await verify_api_key(api_key="my-secret-token", settings=settings)
    assert result == "my-secret-token"


def test_job_create_request_validates_type_pattern() -> None:
    """Job type must follow strict identifier regex."""
    # Valid identifiers
    req1 = JobCreateRequest(type="send_email", payload={"to": "a@b.com"})
    assert req1.type == "send_email"

    req2 = JobCreateRequest(type="worker.task-v1", payload={})
    assert req2.type == "worker.task-v1"

    # Invalid identifiers
    with pytest.raises(ValidationError):
        JobCreateRequest(type="has spaces", payload={})

    with pytest.raises(ValidationError):
        JobCreateRequest(type="path/traversal/../etc", payload={})

    with pytest.raises(ValidationError):
        JobCreateRequest(type="<script>alert(1)</script>", payload={})


def test_job_create_request_enforces_payload_size_limit() -> None:
    """Payloads exceeding MAX_PAYLOAD_BYTES are rejected with ValidationError."""
    # Under limit
    small_req = JobCreateRequest(type="small_job", payload={"msg": "hello"})
    assert small_req.payload["msg"] == "hello"

    # Exceeding 1MB
    huge_data = "x" * (1024 * 1024 + 100)
    with pytest.raises(ValidationError) as exc_info:
        JobCreateRequest(type="huge_job", payload={"data": huge_data})
    assert "exceeds maximum allowed limit" in str(exc_info.value)


def test_job_create_request_validates_numeric_bounds() -> None:
    """Priority, timeout_seconds, and max_retries have safe bounds."""
    # Invalid priority (< -1000 or > 1000)
    with pytest.raises(ValidationError):
        JobCreateRequest(type="valid_type", priority=5000)

    # Invalid timeout (< 1 or > 86400)
    with pytest.raises(ValidationError):
        JobCreateRequest(type="valid_type", timeout_seconds=0)
    with pytest.raises(ValidationError):
        JobCreateRequest(type="valid_type", timeout_seconds=100000)

    # Invalid max_retries (< 0 or > 50)
    with pytest.raises(ValidationError):
        JobCreateRequest(type="valid_type", max_retries=-1)
    with pytest.raises(ValidationError):
        JobCreateRequest(type="valid_type", max_retries=100)
