# ADR-2: Sử Dụng Đồng Hồ Của Database Thay Vì Worker

## Trạng thái
Đã chấp thuận (Accepted)

## Bối cảnh
Trong hệ thống phân tán, các worker chạy trên các tiến trình/máy chủ khác nhau. Đồng hồ vật lý giữa các máy chủ (physical clock) luôn tồn tại độ lệch (clock skew / clock drift) và có thể bị điều chỉnh giật lùi (NTP step). Nếu worker tự dùng `time.time()` hoặc `datetime.now()` để so sánh với `lease_expires_at`, các lỗi race condition và lease expire giả sẽ xảy ra không thể kiểm soát.

## Quyết định
- **Toàn bộ mốc thời gian hết hạn lease và deadline được tính và kiểm tra bằng đồng hồ của database** (`now()` trong PostgreSQL).
- Worker **không bao giờ** tự so sánh giờ cục bộ để quyết định lease còn hạn hay không trong database.
- Worker chỉ dùng đồng hồ đơn điệu cục bộ (`time.monotonic()`) làm timeout bảo vệ sớm (client-side safety margin).
- Mọi logic thời gian trong code Python được trừu tượng hóa qua `Clock` interface để hỗ trợ deterministic unit tests.

## Hệ quả & Đánh đổi
- **Ưu điểm**:
  - Triệt tiêu hoàn toàn rủi ro do clock skew giữa các máy worker.
  - Phán quyết lease hợp lệ chỉ thuộc về 1 thực thể duy nhất (Database).
- **Đánh đổi**:
  - Tăng nhẹ số lần truy vấn tới DB nếu cần kiểm tra lease, nhưng được tích hợp tự nhiên vào chu kỳ Heartbeat (`TTL / 3`).
