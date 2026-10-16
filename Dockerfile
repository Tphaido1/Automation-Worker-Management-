# syntax=docker/dockerfile:1
# Multi-stage production Dockerfile for LeaseFlow

# Stage 1: Dependency builder using official Astral uv
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Copy dependency definition files first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies without installing the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy application source and build project wheel
COPY app ./app
COPY README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# Stage 2: Minimal runtime image
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

# Security: Run as dedicated non-root user
RUN groupadd -r -g 10001 leaseflow && \
    useradd -r -u 10001 -g leaseflow -s /bin/false -d /app leaseflow

# Copy virtual environment and source from builder
COPY --from=builder --chown=leaseflow:leaseflow /app/.venv /app/.venv
COPY --from=builder --chown=leaseflow:leaseflow /app/app /app/app
COPY --chown=leaseflow:leaseflow alembic.ini /app/
COPY --chown=leaseflow:leaseflow migrations /app/migrations

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

USER leaseflow

EXPOSE 8000

# Default command: FastAPI API server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
