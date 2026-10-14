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

## Cài đặt & Bắt đầu Nhanh
```bash
# Cài đặt môi trường ảo và dependencies
uv venv
uv pip install -e ".[dev]"

# Chạy test
pytest

# Kiểm tra lint & format
ruff check .
mypy app
```
