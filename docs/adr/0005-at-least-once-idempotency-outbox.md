# ADR-5: Mô Hình At-Least-Once Delivery, Idempotency và Transactional Outbox

## Trạng thái
Đã chấp thuận (Accepted)

## Bối cảnh
Trong hệ thống phân tán chịu lỗi, cam kết "Exactly-Once Processing" đối với các tác vụ ngoại vi (gửi email, thanh toán, gọi webhooks) là bất khả thi theo định lý FLP và 2-Generals Problem. Nếu worker chết ngay sau khi gọi API bên ngoài nhưng trước khi kịp commit kết quả vào DB, job sẽ được chạy lại và có thể gây trùng lặp tác dụng phụ (duplicate side-effects).

## Quyết định
- Không tuyên bố Exactly-once. Mô hình chính thức của LeaseFlow là:
  **At-Least-Once Delivery + Idempotent Processing + Fencing**.
- Cung cấp `idempotency_key`, `fencing_token` và `attempt` vào `JobContext` cho handler.
- Áp dụng mẫu hình **Transactional Outbox**:
  - Với các tác dụng phụ quan trọng, worker ghi "ý định thực hiện" vào bảng `outbox` **trong cùng transaction** với việc commit kết quả job.
  - Một tiến trình dispatcher riêng rẽ sẽ đọc bảng `outbox`, gửi tác vụ ra ngoài và deduplicate dựa trên `dedup_key`.

## Hệ quả & Đánh đổi
- **Ưu điểm**:
  - Tính nhất quán dữ liệu đạt chuẩn công nghiệp, không bị mất thông điệp.
  - Tách rời thời điểm hoàn tất tính toán nội bộ khỏi thời điểm phát tán ra mạng ngoài.
- **Giới hạn**:
  - Nếu hệ thống ngoài không hỗ trợ idempotency key, vẫn có một cửa sổ nhỏ có thể gây trùng nếu mạng bị ngắt kết nối giữa chừng (được ghi rõ trong tài liệu đảm bảo).
