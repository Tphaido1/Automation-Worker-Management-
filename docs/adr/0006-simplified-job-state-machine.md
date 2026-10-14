# ADR-6: Đơn Giản Hóa State Machine và Tách Biệt Outcome

## Trạng thái
Đã chấp thuận (Accepted)

## Bối cảnh
Nhiều hệ thống hàng đợi thường nhầm lẫn giữa **Trạng thái của Job (Job Lifecycle State)** và **Kết quả của một lần chạy (Execution Outcome)**. Việc đưa các giá trị như `LEASE_EXPIRED`, `TIMED_OUT`, `FAILED` vào làm trạng thái chính của job làm bùng nổ số lượng trạng thái trung gian, gây phức tạp cho việc chuyển đổi trạng thái và dễ sinh ra các góc khuất logic không thể phục hồi.

## Quyết định
1. **Đơn giản hóa Job Status**: Chỉ giữ 7 trạng thái chính:
   - `PENDING`: Đang chờ đến thời điểm chạy (`run_at`).
   - `QUEUED`: Đã sẵn sàng trong hàng đợi để worker claim.
   - `RUNNING`: Đang được một worker giữ lease và thực thi.
   - `SUCCEEDED`: Đã hoàn thành thành công (Terminal).
   - `RETRY_WAITING`: Thất bại tạm thời, đang đợi backoff để quay lại `QUEUED`.
   - `DEAD_LETTER`: Hết số lần retry hoặc lỗi không thể phục hồi, chờ can thiệp thủ công (Terminal có thể retry tay).
   - `CANCELLED`: Người dùng hủy bỏ chủ động (Terminal).
2. **Tách biệt Execution Outcome**:
   - `LEASE_EXPIRED`, `TIMED_OUT`, `REJECTED_STALE`, `FAILED` **không phải trạng thái của job**.
   - Chúng là **Outcome** của một lần chạy cụ thể và được ghi nhận trong bảng lịch sử `executions`.
   - Sau các outcome này, job sẽ tự động chuyển sang `RETRY_WAITING` hoặc `DEAD_LETTER`.
3. **Cài đặt tập trung**:
   - Mọi quy tắc chuyển trạng thái được gom vào một hàm duy nhất `transition(job, new_status)` trong domain module, cấm rải rác các câu `if` đổi trạng thái tùy tiện.

## Hệ quả & Đánh đổi
- **Ưu điểm**:
  - State machine tinh gọn, dễ chứng minh tính đúng đắn toán học bằng Property-Based Testing (Hypothesis).
  - Lịch sử thực thi chi tiết, rõ ràng nguyên nhân thất bại ở từng attempt mà không làm xáo trộn vòng đời job.
