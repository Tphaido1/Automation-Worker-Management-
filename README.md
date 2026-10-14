# LeaseFlow

> **Distributed Job Scheduler chịu lỗi bằng Python.**

LeaseFlow là hệ thống lập lịch và phân phối công việc chịu lỗi (fault-tolerant distributed job scheduler) được xây dựng trên nền tảng Python, PostgreSQL và kiến trúc Leases + Monotonic Fencing Tokens.

## 5 Tính chất Cốt lõi (Core Guarantees)
- **G1 (Không mất job)**: Phục hồi tự động khi worker chết đột ngột (`kill -9`).
- **G2 (Không ghi đè kép)**: Ngăn chặn hoàn toàn zombie worker bằng Fencing Token đơn điệu.
- **G3 (Không retry vô hạn)**: Exponential Backoff với Full Jitter và Dead-Letter Queue (DLQ).
- **G4 (Minh bạch trạng thái)**: State machine chặt chẽ và lưu vết toàn bộ lịch sử `executions`.
- **G5 (Idempotency)**: Đảm bảo tác dụng phụ an toàn với `idempotency_key` và Transactional Outbox pattern.

## Yêu cầu Hệ thống
- Python >= 3.11 (khuyên dùng Python 3.12 / 3.13)
- [uv](https://docs.astral.sh/uv/) làm package manager
- PostgreSQL 16+ (hoặc Docker Compose)

## Tiến độ Triển khai (Roadmap & Status)

- [x] **Phase 0: Nền tảng dự án**: Docker Compose, Alembic, CI GitHub Actions, ADR 1-6, Clock deterministic, Pyproject.
- [x] **Phase 1: Job CRUD & State Machine**: State machine chuyển trạng thái nghiêm ngặt ($7 \times 7$), Models, Repository, REST API (`POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`, `GET /jobs/{id}/executions`).
- [x] **Phase 2: Safe Claim & Worker Runtime**: Promoter (`PENDING -> QUEUED`), `PostgresQueueBackend` với `SELECT ... FOR UPDATE SKIP LOCKED`, `Worker` loop hỗ trợ async & sync handlers qua ThreadPoolExecutor, Concurrency Claim Tests (20 workers cùng đua 500 jobs - 0 duplicate claim, 0 lost jobs).
- [ ] **Phase 3: Lease, Heartbeat & Reaper**: Lease heartbeat gia hạn độc lập, Reaper thu hồi lease quá hạn / timeout.
- [ ] **Phase 4: Fencing Token Enforcement**: Ngăn chặn hoàn toàn zombie worker ghi đè kết quả.
- [ ] **Phase 5: Exponential Backoff, Jitter & DLQ**: Chiến lược retry tất định với full jitter.
- [ ] **Phase 6: Transactional Outbox Pattern**: Bảo đảm tính toàn vẹn tác dụng phụ (side-effects).
- [ ] **Phase 7: Observability**: Prometheus metrics & Grafana dashboard.
- [ ] **Phase 8: Chaos Engineering & Property Testing**: Fault injection, process pause (`SIGSTOP`/`SIGCONT`), kill (`SIGKILL`).

## Cài đặt & Bắt đầu Nhanh
```bash
# Cài đặt môi trường ảo và dependencies với uv
uv sync

# Chạy toàn bộ test suite (Unit, Integration, Concurrency)
uv run pytest -v

# Kiểm tra lint, format & strict type check
uv run ruff check .
uv run mypy app
```

