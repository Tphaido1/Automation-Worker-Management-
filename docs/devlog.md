# LeaseFlow Development Log (DevLog)

Nhật ký hành trình phát triển và hoàn thiện dự án **LeaseFlow** theo phương pháp luận từng Milestone có Definition of Done (DoD).

---

## Phase 0: Nền Tảng Dự Án (Foundations)
- **Hạng mục hoàn thành:**
  - Cấu hình môi trường phát triển với `uv`, `pyproject.toml`, strict type-checking (`mypy strict`), linter/formatter `ruff`.
  - Thiết lập hạ tầng container với Docker Compose (`PostgreSQL 16`).
  - Cấu hình Alembic migrations và Async SQLAlchemy 2.0 engine.
  - Ban hành 6 Architecture Decision Records (ADR-1 đến ADR-6) định hình toàn bộ thiết kế hệ thống.
  - Xây dựng trừu tượng `Clock` hỗ trợ kiểm thử tất định (deterministic time travel).
- **Kết quả DoD:** Pipeline CI, migrations chạy trơn tru, lint/type check 100% clean.

---

## Phase 1: Job CRUD & Pure State Machine
- **Hạng mục hoàn thành:**
  - Thiết kế State Machine thuần túy ($7 \times 7$) độc lập với I/O trong `app/domain/state_machine.py`.
  - Kiểm thử toàn diện 49 tổ hợp chuyển trạng thái (11 hợp lệ, 38 bất hợp lệ).
  - Triển khai `JobModel` và `ExecutionModel` với UUID v4 và JSONB.
  - Xây dựng tầng Repository với Idempotency Key chống tạo trùng lặp.
  - Xây dựng REST API bằng FastAPI: `POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`, `GET /jobs/{id}/executions`.
- **Kết quả DoD:** 22 unit & integration tests pass 100%.

---

## Phase 2: Safe Claim & Worker Runtime
- **Hạng mục hoàn thành:**
  - Triển khai `PromoterLoop` quét và đẩy các jobs đến hạn từ `PENDING -> QUEUED`.
  - Triển khai `PostgresQueueBackend` với `SELECT ... FOR UPDATE SKIP LOCKED` và Partial Index.
  - Xây dựng `Worker` runtime hỗ trợ cả async coroutines và sync handlers qua `ThreadPoolExecutor`.
  - Viết bộ kiểm thử tương tranh (concurrency stress test): 20 workers đồng thời claim 500 jobs.
- **Kết quả DoD:** 0 duplicate claim, 0 lost jobs trong mọi lần chạy thử nghiệm tương tranh.

---

## Phase 3: Lease, Heartbeat & Reaper
- **Hạng mục hoàn thành:**
  - Tách biệt `lease_expires_at` (30s) và `execution_deadline` (started_at + timeout) theo ADR-4.
  - Xây dựng background heartbeat task độc lập trong worker định kỳ gia hạn lease sau mỗi `TTL / 3`.
  - Xây dựng tiến trình `ReaperLoop` quét và thu hồi lease quá hạn hoặc các jobs bị timeout logic.
- **Kết quả DoD:** Kiểm thử mô phỏng worker crash giữa chừng; Reaper phát hiện và phục hồi job cho worker khác trong vòng $\le TTL + REAPER\_INTERVAL$.

---

## Phase 4: Monotonic Fencing Token Enforcement
- **Hạng mục hoàn thành:**
  - Cấp phát `fencing_token` đơn điệu tăng dần tại mỗi lượt claim.
  - Bắt buộc kiểm tra token trên mọi câu lệnh UPDATE của worker (heartbeat, complete, fail).
  - Tự động ghi nhận `REJECTED_STALE` khi token không khớp.
- **Kết quả DoD:** Tái hiện kịch bản Zombie Worker (Worker cũ bị đóng băng rồi tỉnh dậy cố commit); 100% stale commit bị database từ chối an toàn.

---

## Phase 5: Exponential Backoff, Jitter & Dead-Letter Queue (DLQ)
- **Hạng mục hoàn thành:**
  - Cài đặt thuật toán Exponential Backoff với Full Jitter chống bão retry (Thundering Herd).
  - Cơ chế phân loại lỗi: Retryable vs Non-retryable errors.
  - Tự động chuyển các job vượt quá `max_retries` vào trạng thái `DEAD_LETTER`.
  - Cung cấp API quản trị: `POST /jobs/{id}/retry` để operator giải phóng job khỏi DLQ, và endpoint `GET /dead-letters`.
- **Kết quả DoD:** Core Guarantee G3 được chứng minh bằng test tự động.

---

## Phase 6: Transactional Outbox Pattern
- **Hạng mục hoàn thành:**
  - Triển khai bảng `outbox` và model SQLAlchemy `OutboxModel`.
  - Worker emit side-effects vào context và commit nguyên tử trong cùng 1 transaction với status `SUCCEEDED`.
  - Xây dựng `OutboxDispatcher` quét và dispatch events bằng `FOR UPDATE SKIP LOCKED`.
  - Triển khai `FakeSMTPServer` với kiểm tra trùng lặp qua `dedup_key`.
- **Kết quả DoD:** Chứng minh cam kết G5: khi worker crash trước commit, 0 side-effects rò rỉ ra ngoài.

---

## Phase 7: Observability & Production Telemetry
- **Hạng mục hoàn thành:**
  - 12 Prometheus metrics chuẩn hoá (độ sâu hàng đợi, throughput counters, claim/heartbeat latency histograms, zombie rejection & lease expiration counters).
  - Endpoint `/metrics` scrape dữ liệu thời gian thực.
  - Structured JSON logging với correlation contextvars (`job_id`, `worker_id`, `fencing_token`).
  - Tự động provisioning Grafana dashboard chuyên sâu `deploy/grafana/dashboards/leaseflow-overview.json`.
- **Kết quả DoD:** Dashboard quan sát đầy đủ mọi khía cạnh hoạt động của hệ thống.

---

## Phase 8: Chaos Engineering & Property-based Testing
- **Hạng mục hoàn thành:**
  - Xây dựng OS-level Chaos Harness với `psutil` hỗ trợ `SIGKILL`, `SIGSTOP` và `SIGCONT`.
  - Mô phỏng lỗi mạng và transient drops qua Fault Injection Proxy.
  - Kiểm thử bất biến phân tán với Hypothesis `RuleBasedStateMachine` kiểm tra liên tục 8 bất biến.
  - Script kiểm thử chịu tải kéo dài `scripts/soak_test.py`.
- **Kết quả DoD:** Bộ 84/84 tests pass 100%, duy trì toàn vẹn 8 bất biến dưới điều kiện chaos liên tục.

---

## Phase 9: Demo, Benchmark & Hoàn Thiện Tài Liệu
- **Hạng mục hoàn thành:**
  - Script demo tự động theo kịch bản Mục 7: [`scripts/demo.py`](file:///c:/Python/leaseflow/scripts/demo.py) (`make demo`).
  - Hệ thống benchmark đa tiêu chí: [`scripts/bench.py`](file:///c:/Python/leaseflow/scripts/bench.py) (`make bench`) đo throughput scaling, claim/e2e latency percentiles, `SKIP LOCKED` scalability, và recovery time.
  - Báo cáo hiệu năng thực nghiệm: [`docs/benchmark-results.md`](file:///c:/Python/leaseflow/docs/benchmark-results.md).
  - Tài liệu thiết kế kiến trúc toàn diện kèm sơ đồ Mermaid: [`docs/architecture.md`](file:///c:/Python/leaseflow/docs/architecture.md).
  - Bài kỹ thuật chuyên sâu về các lỗi đua tranh đã gặp và giải quyết: [`docs/failure-report.md`](file:///c:/Python/leaseflow/docs/failure-report.md).
  - Cập nhật tài liệu showcase dự án [`README.md`](file:///c:/Python/leaseflow/README.md).
- **Kết quả DoD:** Người lạ clone repo chỉ cần chạy `make demo` hoặc `make bench` là thấy toàn bộ kịch bản và kết quả rõ ràng mà không cần cấu hình phức tạp.

---

## Phase 10: Hoàn Thiện MVP & Phát Hành v0.1 (Hardening & Release)
- **Hạng mục hoàn thành:**
  - **Rà soát bảo mật & Xác thực API Key:**
    - Cài đặt dependency xác thực `verify_api_key` trong `app/api/security.py` sử dụng hàm so sánh hằng số thời gian `secrets.compare_digest` chống timing attacks.
    - Hỗ trợ cả `X-API-Key` header và `api_key` query parameter.
    - Cấu hình linh hoạt qua `Settings`: `API_KEY`, `REQUIRE_API_KEY` (mặc định permissive mode cho môi trường local/test, kích hoạt an toàn cho production).
    - Bảo vệ các endpoints `/jobs`, `/dead-letters`; giữ mở các endpoints hạ tầng `/healthz`, `/readyz`, `/metrics`.
  - **Kiểm soát kích thước Payload & Ràng buộc đầu vào:**
    - Giới hạn payload tối đa 1MB (`MAX_PAYLOAD_BYTES = 1_048_576`) với Pydantic validator trả mã lỗi 422 chi tiết.
    - Khống chế regex nghiêm ngặt cho `type`: `^[a-zA-Z0-9_\-\.]+$` ngăn chặn SQL/command injection qua tên task.
    - Đặt cận biên cho các tham số số học: `priority` $\in [-1000, 1000]$, `timeout_seconds` $\in [1, 86400]$, `max_retries` $\in [0, 50]$.
  - **Kiểm thử Bảo mật:**
    - Viết 7 unit tests (`tests/unit/test_security.py`) và 4 integration tests (`tests/integration/test_security_api.py`) kiểm tra 401 Unauthorized, 403 Forbidden, 422 Validation Error, và permissive mode.
  - **Vệ sinh Migrations:**
    - Kiểm tra chuỗi migrations (`0001_create_jobs_and_executions.py` -> `0002_create_outbox_table.py`) đạt tính liên tục, tuần tự và an toàn.
  - **Đóng gói Docker & Docker Compose:**
    - Xây dựng `Dockerfile` multi-stage tối ưu với Astral `uv` builder và Python 3.12 slim bookworm runtime, chạy dưới user phi đặc quyền `leaseflow:10001`.
    - Tạo file `.dockerignore` loại trừ triệt để cache, virtualenv, docs, tests, và cơ sở dữ liệu tạm khỏi build context.
    - Cập nhật `docker-compose.yml` tích hợp trọn vẹn cả cụm phân tán gồm `db` (Postgres 16), `api` (FastAPI với auto-migration), `worker` (distributed runner), `prometheus` và `grafana`.
  - **Tài liệu Phát hành & Version Tag:**
    - Soạn thảo `CHANGELOG.md` chuẩn Keep a Changelog theo Semantic Versioning ghi lại chi tiết mọi tính năng từ Phase 0 đến Phase 10.
    - Gắn tag git cục bộ `v0.1.0`.
- **Kết quả DoD:** Toàn bộ 95/95 tests pass 100%, strict type checking (`mypy app`) và code formatting/linting (`ruff`) đạt 100% clean. Hoàn thành trọn vẹn mục tiêu MVP v0.1.0.

