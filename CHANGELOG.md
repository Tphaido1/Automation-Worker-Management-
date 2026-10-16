# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-16

### Added
- **Core Architecture & Storage:**
  - PostgreSQL 16 + SQLAlchemy 2.0 async engine with Alembic migrations (`0001_create_jobs_and_executions`, `0002_create_outbox_table`).
  - Seamless dual-mode storage engine supporting PostgreSQL in production and SQLite WAL mode for local developer environments without external dependencies.
- **State Machine & Concurrency Control:**
  - Strict 7-state lifecycle state machine (`PENDING`, `SCHEDULED`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`, `DEAD_LETTER`).
  - Distributed lease acquisition using PostgreSQL `FOR UPDATE SKIP LOCKED` row-level locks preventing race conditions across multi-node worker pools.
  - Heartbeat-driven lease renewal mechanism ensuring leased jobs are protected from premature reclamation during long-running execution.
- **Worker Engine & Self-Healing:**
  - Async `WorkerPool` with configurable concurrency, execution timeouts, and graceful shutdown (SIGINT/SIGTERM signal interception with drain timeouts).
  - Background `Reaper` process recovering zombie jobs when workers crash, fail heartbeats, or encounter network partitions.
  - Background `Promoter` process for delayed/scheduled jobs (`SCHEDULED` -> `PENDING`).
  - Exponential backoff retry engine with full jitter (`base_delay * 2^attempt + jitter`).
  - Dead-Letter Queue (DLQ) isolating unrecoverable jobs exceeding `max_retries`.
- **Reliability & Event Outbox:**
  - Transactional Outbox pattern (`outbox_events` table) providing at-least-once event delivery guarantees without distributed 2PC transactions.
- **Observability & Diagnostics:**
  - Prometheus metrics instrumentation (`/metrics`) exposing queue depths, state transitions, lease durations, worker heartbeats, and reaper sweeps.
  - Preconfigured Grafana dashboard and Prometheus scraping configuration in `deploy/`.
  - Structured JSON logging with context-bound correlation IDs (`job_id`, `worker_id`).
- **RESTful Management API:**
  - FastAPI endpoints for job enqueueing, inspection, cancellation, DLQ inspection, and retry replay.
  - Production readiness probes (`/healthz`, `/readyz`).
- **Security & Hardening:**
  - API Key authentication dependency (`verify_api_key`) using constant-time string comparison (`secrets.compare_digest`) via `X-API-Key` header or query parameter.
  - Strict Pydantic input validation on job types (regex `^[a-zA-Z0-9_\-\.]+$`), payload size limits (1MB maximum payload), and bounded numeric ranges for priority, retries, and timeouts.
- **Packaging & Deployment:**
  - Production-grade multi-stage `Dockerfile` using Astral `uv` for fast, reproducible builds and running under dedicated non-root user `leaseflow:10001`.
  - Complete Docker Compose environment with Postgres, API, Worker, Prometheus, and Grafana services.
- **Validation, Benchmarking & Tooling:**
  - Interactive demonstration runner (`scripts/demo.py`) showcasing full lifecycle, retries, DLQ, and reaper recovery.
  - High-throughput stress benchmark tool (`scripts/bench.py`) evaluating enqueue, claim, execute, and roundtrip performance.
  - Chaos simulation suite verifying resilience under simulated worker crashes, deadlocks, and network failures.

[0.1.0]: https://github.com/leaseflow/leaseflow/releases/tag/v0.1.0
