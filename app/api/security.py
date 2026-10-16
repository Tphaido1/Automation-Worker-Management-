import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import Settings, get_settings

API_KEY_HEADER = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="API Key for authenticating client requests",
)


async def verify_api_key(
    api_key: Annotated[str | None, Security(API_KEY_HEADER)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> str | None:
    """
    Validates the provided API key against configured server settings.

    Authentication policy:
      - If REQUIRE_API_KEY is False and API_KEY is None: authentication is disabled (permissive mode).
      - If REQUIRE_API_KEY is True or API_KEY is set:
          - Missing header -> HTTP 401 Unauthorized
          - Invalid key (constant-time comparison) -> HTTP 403 Forbidden
    """
    # If auth is not required and no API_KEY configured, allow all
    if not settings.REQUIRE_API_KEY and not settings.API_KEY:
        return api_key

    # Check presence
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is missing in 'X-API-Key' header",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Constant-time comparison to prevent timing attacks
    expected_key = settings.API_KEY or ""
    if not secrets.compare_digest(api_key, expected_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key provided",
        )

    return api_key
