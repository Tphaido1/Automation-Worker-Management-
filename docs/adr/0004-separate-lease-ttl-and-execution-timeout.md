# ADR-4: Tách Biệt Lease TTL và Execution Timeout

## Trạng thái
Đã chấp thuận (Accepted)

## Bối cảnh
Trong các hệ thống phân tán truyền thống, các khái niệm về "thời gian chạy tối đa" và "thời gian phát hiện worker chết" thường bị gộp chung vào một biến timeout. Điều này dẫn tới hai nghịch lý:
1. Nếu timeout quá ngắn: Các tác vụ dài hợp lệ bị hủy oan uổng.
2. Nếu timeout quá dài: Khi một worker bị crash đột ngột (`kill -9`), hệ thống phải mất rất nhiều thời gian (vài chục phút) mới phát hiện và chạy lại job.

## Quyết định
Tách biệt thành hai cơ chế độc lập:
1. **Lease TTL (`lease_expires_at`)**:
   - Thời gian tối đa hệ thống chấp nhận **không nghe tin (heartbeat)** từ worker (mặc định 30 giây).
   - Worker phải gửi heartbeat định kỳ (`TTL / 3`, ví dụ 10 giây) để gia hạn.
   - Giải quyết bài toán: **Worker chết hoặc mất kết nối mạng**.
2. **Execution Timeout (`execution_deadline`)**:
   - Thời gian tối đa một lần chạy được phép kéo dài (`started_at + timeout_seconds`), bất kể worker có heartbeat đều hay không.
   - Giải quyết bài toán: **Job bị treo vòng lặp vô hạn hoặc deadlock logic**.

## Hệ quả & Đánh đổi
- **Ưu điểm**:
  - Phát hiện worker chết nhanh chóng (trong vòng `TTL + REAPER_INTERVAL`), giảm thiểu Recovery Time.
  - Vẫn cho phép job chạy lâu nếu cần thiết, miễn là worker vẫn còn sống và heartbeat đều đặn.
