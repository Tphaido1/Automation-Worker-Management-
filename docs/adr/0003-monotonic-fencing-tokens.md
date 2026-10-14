# ADR-3: Fencing Token Tăng Đơn Điệu Theo Từng Job

## Trạng thái
Đã chấp thuận (Accepted)

## Bối cảnh
Khi một worker nhận job nhưng bị tạm dừng (process pause do GC, mạng lag, `SIGSTOP`), lease của nó có thể hết hạn. Reaper sẽ thu hồi job và trao cho một worker khác. Khi worker cũ thức giấc (zombie worker), nó vẫn nghĩ mình đang giữ quyền và có thể commit đè lên kết quả của worker mới, vi phạm tính toàn vẹn dữ liệu (split-brain write).

## Quyết định
- Mỗi bản ghi `jobs` có một cột số nguyên `fencing_token BIGINT NOT NULL DEFAULT 0`.
- Mỗi lần claim job thành công, `fencing_token` được tăng đơn điệu: `fencing_token = fencing_token + 1`.
- **Mọi thao tác ghi của worker** (heartbeat, commit thành công, báo lỗi) bắt buộc phải kèm theo điều kiện:
  `WHERE id = :job_id AND fencing_token = :my_token`.
- Nếu câu lệnh UPDATE trả về `rowcount == 0`, chứng tỏ token đã lỗi thời (stale), worker bị tước quyền và ghi outcome `REJECTED_STALE` vào `executions`.

## Hệ quả & Đánh đổi
- **Ưu điểm**:
  - Bảo vệ tuyệt đối không bao giờ có 2 worker cùng commit kết quả cho 1 job trong Database (Đạt mục tiêu G2).
  - Tái hiện và kiểm chứng được thông qua kịch bản kiểm thử lỗi `SIGSTOP/SIGCONT`.
- **Lưu ý**:
  - Fencing token trong DB chỉ bảo vệ các tác vụ ghi vào LeaseFlow DB. Với các tác dụng phụ bên ngoài (gửi email, gọi external API), cần bổ sung Idempotency Key và Transactional Outbox (ADR-5).
