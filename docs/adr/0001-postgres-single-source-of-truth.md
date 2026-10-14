# ADR-1: PostgreSQL là Nguồn Sự Thật Duy Nhất cho MVP

## Trạng thái
Đã chấp thuận (Accepted)

## Bối cảnh
LeaseFlow là hệ thống Distributed Job Scheduler chịu lỗi, cần lưu trữ trạng thái công việc, cơ chế phân phối hàng đợi (queue), cấp phát và gia hạn lease, quản lý fencing token và lịch sử thực thi.
Có hai lựa chọn kiến trúc phổ biến:
1. Dùng Redis làm broker/queue kết hợp Postgres làm persistent storage.
2. Dùng PostgreSQL làm nguồn sự thật duy nhất (Single Source of Truth) kết hợp cơ chế `SELECT ... FOR UPDATE SKIP LOCKED`.

## Quyết định
Chọn **PostgreSQL là nguồn sự thật duy nhất** cho toàn bộ hệ thống MVP:
- Hàng đợi (queue), trạng thái job, lease, fencing token và lịch sử `executions` đều nằm trong PostgreSQL.
- Claim job sử dụng mẫu hình `SELECT ... FOR UPDATE SKIP LOCKED`.
- Thiết kế một interface trừu tượng `QueueBackend` để có thể mở rộng sang Redis Streams hoặc NATS JetStream ở các giai đoạn nâng cao (Phase 11).

## Hệ quả & Đánh đổi
- **Ưu điểm**:
  - Tính nguyên tử tuyệt đối (ACID): Thao tác "nhận job + cấp lease + tăng fencing token" diễn ra trong **một transaction duy nhất**, loại bỏ hoàn toàn bài toán phân tán đồng bộ dữ liệu giữa 2 hệ thống (Dual-write problem).
  - Đơn giản hóa hạ tầng vận hành: Chỉ cần 1 CSDL Postgres duy nhất.
- **Hạn chế & Cách giảm thiểu**:
  - Ghi nhận `heartbeat` và `UPDATE` liên tục có thể gây bloat bảng và tải I/O.
  - *Giải pháp*: Sử dụng Partial Indexes (`WHERE status = 'QUEUED'`, `WHERE status = 'RUNNING'`), tinh chỉnh autovacuum và chỉ lưu trữ các trường tối giản.
