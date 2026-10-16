# Tính Bất Biến & Cam Kết Idempotency (Core Guarantee G5)

Tài liệu này xác định rõ ràng, minh bạch **những gì LeaseFlow đảm bảo** và **những gì LeaseFlow không đảm bảo** liên quan đến việc thực thi tác dụng phụ ngoại vi (side-effects: gửi email, gọi webhook, thanh toán).

---

## 1. Mô Hình Tổng Thể: Tại Sao Không Thể Có "Exactly-Once"?

Theo **Định lý FLP (Fischer-Lynch-Paterson)** và **Bài toán Hai Vị Tướng (Two Generals' Problem)**, trong một hệ thống phân tán qua mạng không đáng tin cậy:
> Không thể đảm bảo chắc chắn tuyệt đối rằng một tác vụ ngoại vi chỉ được kích hoạt đúng một lần duy nhất nếu không có sự phối hợp trạng thái hai chiều.

Mô hình kiến trúc chính thức của LeaseFlow là:
$$\textbf{At-Least-Once Delivery} + \textbf{Monotonic Fencing} + \textbf{Transactional Outbox} + \textbf{Idempotent Receivers}$$

---

## 2. Những Gì LeaseFlow ĐẢM BẢO

### ✅ Cam kết 1: Tính Nguyên Tử Giữa Kết Quả Tính Toán và Ý Định Phát Tán (Transactional Outbox)
- Khi handler gọi `ctx.emit_outbox(kind="email", payload=..., dedup_key=...)`, sự kiện được đưa vào hàng đợi tạm thời trong bộ nhớ của worker.
- Việc ghi nhận công việc thành công (`status = 'SUCCEEDED'`) và việc chèn bản ghi vào bảng `outbox` được thực hiện **trong cùng một Database Transaction duy nhất**.
- **Hệ quả**:
  - Không bao giờ xảy ra tình trạng job báo thành công nhưng sự kiện ngoại vi bị mất.
  - Nếu worker bị sập (`kill -9`) hoặc bị hủy giữa chừng, transaction bị rollback hoàn toàn $\to$ **không có bất kỳ bản ghi outbox nào bị phát tán rác**.

### ✅ Cam kết 2: Ngăn Chặn Hoàn Toàn Zombie Worker Tạo Tác Dụng Phụ
- Việc commit kết quả job và chèn outbox đều có rào chắn `WHERE fencing_token = :token`.
- Nếu một worker A bị treo (ví dụ full GC, mạng lag), lease hết hạn, và Reaper đã giao job cho worker B (với `fencing_token = 2`):
  - Khi worker A tỉnh dậy và cố gắng commit kèm outbox events (với `fencing_token = 1`), câu lệnh SQL trả về `rowcount = 0`.
  - Toàn bộ outbox event của worker A bị hủy bỏ tức thì (`REJECTED_STALE`). Worker B tiếp quản an toàn.

### ✅ Cam kết 3: Tính Lặp Lại Tất Định với `dedup_key`
- Mọi bản ghi trong bảng `outbox` đều mang một khóa duy nhất `dedup_key` (có ràng buộc `UNIQUE` trên database).
- Nếu handler không truyền `dedup_key`, LeaseFlow tự động sinh theo mẫu:
  `{idempotency_key or job_id}:{kind}`
- Đảm bảo một công việc cụ thể không thể sinh ra hai bản ghi outbox trùng lặp trong cơ sở dữ liệu.

---

## 3. Những Gì LeaseFlow KHÔNG ĐẢM BẢO & Cửa Sổ Rủi Ro (The Vulnerability Window)

Hệ thống **không thể** ngăn chặn gửi trùng trong hai kịch bản sau:

### ⚠️ Kịch bản 1: Handler Tự Thực Hiện I/O Trực Tiếp (Không Dùng Outbox)
- **Mô tả**: Nếu lập trình viên gọi trực tiếp thư viện gửi email hoặc API thanh toán (ví dụ `requests.post("https://api.stripe.com/...")`) ngay bên trong thân hàm worker handler thay vì dùng `ctx.emit_outbox(...)`:
- **Cửa sổ rủi ro**:
  1. Worker gửi API sang Stripe thành công (tiền đã trừ).
  2. Ngay trước khi worker kịp gửi câu lệnh `complete()` về database Postgres, worker bị mất điện hoặc `kill -9`.
  3. Lease hết hạn, Reaper phát hiện và đưa job về `RETRY_WAITING`.
  4. Worker khác nhận lại job và lại gọi Stripe lần thứ hai.
- **Khuyến nghị**: **Tuyệt đối không thực hiện I/O có tác dụng phụ trực tiếp trong worker handler**. Luôn ủy thác qua `ctx.emit_outbox()`.

### ⚠️ Kịch bản 2: Hệ Thống Tiếp Nhận Bên Ngoài Không Hỗ Trợ Idempotency
- **Mô tả**: `OutboxDispatcher` đọc bảng `outbox` và gửi gói tin sang hệ thống ngoại vi (ví dụ đối tác webhook không kiểm tra dedup).
- **Cửa sổ rủi ro**:
  1. Dispatcher gửi HTTP POST payload kèm header `Idempotency-Key: {dedup_key}`.
  2. Máy chủ đối tác nhận được, xử lý xong, nhưng đường truyền mạng bị đứt trước khi ACK/HTTP 200 quay về Dispatcher.
  3. Dispatcher gặp timeout hoặc crash trước khi kịp đánh dấu `dispatched_at = now()`.
  4. Lần quét tiếp theo, Dispatcher gửi lại gói tin. Nếu máy chủ đối tác không kiểm tra `Idempotency-Key`, đối tác sẽ xử lý lần thứ hai.
- **Khuyến nghị**: Hệ thống bên ngoài (hoặc fake SMTP server trong test) bắt buộc phải kiểm tra và deduplicate dựa trên `dedup_key`.

---

## 4. Bảng So Sánh Chiến Lược

| Tiêu chí | Trực tiếp gọi API trong Worker | Dùng Transactional Outbox (LeaseFlow) |
| :--- | :---: | :---: |
| **Bảo toàn khi worker chết trước commit** | ❌ Gửi lặp tác dụng phụ | ✅ An toàn tuyệt đối (Rollback outbox) |
| **Bảo vệ trước Zombie Worker** | ❌ Zombie vẫn gọi API ngoại vi | ✅ Fencing token chặn đứng việc ghi outbox |
| **Khả năng Retry độc lập** | ❌ Phải chạy lại toàn bộ tính toán | ✅ Chỉ retry việc gửi bản ghi outbox |
| **Tách rời độ trễ mạng** | ❌ Worker bị nghẽn bởi API bên ngoài | ✅ Worker commit nhanh, Dispatcher gửi bất đồng bộ |
