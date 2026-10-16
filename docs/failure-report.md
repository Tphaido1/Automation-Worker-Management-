# Những Bug Tôi Gặp: Lỗi Đua Tranh & Bài Toán Phân Tán trong LeaseFlow

> *"Trong các hệ thống phân tán, mọi điều tồi tệ có thể xảy ra đều sẽ xảy ra: mạng bị chậm, tiến trình bị đóng băng bởi GC pause, máy chủ bị rút nguồn, và worker có thể trở thành thây ma bất cứ lúc nào."*

Tài liệu này ghi lại chi tiết các lỗi đua tranh (race conditions), bài toán bế tắc và sai lầm kiến trúc thực tế được phát hiện và giải quyết trong quá trình xây dựng hệ thống **LeaseFlow**, kèm theo phân tích nguyên nhân gốc rễ và kiểm thử tự động để ngăn ngừa tái diễn.

---

## 1. Bug 1: Thây Ma Trỗi Dậy (Zombie Worker Commit Race Condition)

### Triệu chứng & Tác hại
Worker A nhận xử lý một tác vụ thanh toán. Đột nhiên máy chủ của Worker A gặp hiện tượng "Stop-the-world" do Garbage Collection (hoặc kết nối mạng bị đóng băng). Trong thời gian Worker A bị treo, `lease_expires_at` của nó hết hạn. Tiến trình Reaper phát hiện và đưa job về hàng đợi. Worker B nhận lại job, xử lý xong và commit kết quả thành công.

Ngay sau đó, Worker A "tỉnh dậy". Nó không biết mình đã bị mất lease và vui vẻ gửi lệnh commit kết quả cũ lên database. Hậu quả: **Dữ liệu của Worker B bị ghi đè, tác vụ bị commit hai lần (vi phạm nghiêm trọng cam kết G2).**

### Nguyên nhân gốc rễ
Câu lệnh cập nhật trạng thái ban đầu chỉ lọc theo ID:
```sql
-- TRƯỚC KHI SỬA (NGUY HIỂM)
UPDATE jobs 
SET status = 'SUCCEEDED', result = :res 
WHERE id = :job_id;
```
Bất kỳ worker nào giữ job ID đều có thể ghi đè kết quả, bất kể lease của nó đã quá hạn từ lâu.

### Giải pháp kỹ thuật: Monotonic Fencing Token (ADR-3)
Gán cho mỗi lượt claim một số thứ tự tăng đơn điệu (`fencing_token = fencing_token + 1`). Mọi thao tác ghi của worker bắt buộc phải đối chiếu token:
```sql
-- SAU KHI SỬA (BẢO VỆ CHẮC CHẮN)
UPDATE jobs 
SET status = 'SUCCEEDED', result = :res, updated_at = now() 
WHERE id = :job_id AND fencing_token = :worker_token;
```
Khi Worker A tỉnh dậy và commit với `fencing_token = 1`, database nhận thấy token hiện tại trong bảng đã là `2`. Câu lệnh trả về `rowcount = 0`. Worker runtime phát hiện token đã cũ, hủy bỏ kết quả và ghi nhận sự kiện `REJECTED_STALE`.

```python
# app/queue/postgres.py
if result.rowcount == 0:
    logger.warning("Commit rejected for job %s: stale fencing token %d", job_id, fencing_token)
    await self._record_execution_outcome(job_id, fencing_token, ExecutionOutcome.REJECTED_STALE)
    return False
```

### Kiểm thử xác thực
- [`tests/integration/test_zombie_fencing.py`](file:///c:/Python/leaseflow/tests/integration/test_zombie_fencing.py)
- [`tests/chaos/test_chaos_zombie_pause.py`](file:///c:/Python/leaseflow/tests/chaos/test_chaos_zombie_pause.py) (Sử dụng `SIGSTOP` và `SIGCONT` thực tế trên OS).

---

## 2. Bug 2: Thắt Cổ Chai Hàng Đợi (Queue Lock Contention)

### Triệu chứng & Tác hại
Khi mở rộng quy mô lên 20–50 workers cùng nhận việc, hệ thống xuất hiện hiện tượng sụt giảm thông lượng nghiêm trọng. Các workers liên tục bị timeout hoặc gặp lỗi serialization failure từ PostgreSQL.

### Nguyên nhân gốc rễ
Câu lệnh claim ban đầu sử dụng:
```sql
SELECT * FROM jobs 
WHERE status = 'QUEUED' 
ORDER BY priority DESC, run_at ASC 
LIMIT 1 
FOR UPDATE;
```
Khi Worker 1 khóa dòng đầu tiên của hàng đợi, tất cả 19 workers còn lại đều bị block và phải xếp hàng chờ đợi Worker 1 hoàn tất transaction.

### Giải pháp kỹ thuật: `FOR UPDATE SKIP LOCKED` & Partial Index
Bổ sung `SKIP LOCKED` cho phép các workers đồng thời bỏ qua các dòng đang bị khóa và nhận ngay các dòng khả dụng tiếp theo mà không phải chờ đợi:
```sql
SELECT id FROM jobs 
WHERE status = 'QUEUED' AND run_at <= now() 
ORDER BY priority DESC, run_at ASC 
LIMIT 1 
FOR UPDATE SKIP LOCKED;
```
Đồng thời tạo **Partial Index** chỉ đánh chỉ mục trên các dòng có `status = 'QUEUED'`:
```sql
CREATE INDEX idx_jobs_queue_claim 
ON jobs (priority DESC, run_at ASC) 
WHERE status = 'QUEUED';
```
Kích thước index này cực nhỏ và không tăng theo số lượng jobs đã hoàn thành trong quá khứ.

### Kiểm thử xác thực
- [`tests/integration/test_concurrency_claim.py`](file:///c:/Python/leaseflow/tests/integration/test_concurrency_claim.py): 20 workers cùng đua nhận 500 jobs $\to$ 100% jobs được nhận, 0 duplicate claim, 0 lost jobs.

---

## 3. Bug 3: Tác Dụng Phụ Bị Nhân Đôi (Dual-Write Problem)

### Triệu chứng & Tác hại
Trong một handler gửi email thông báo đơn hàng, Worker gọi hàm `smtp.send_email()` thành công. Nhưng ngay trước khi câu lệnh `await session.commit()` được gửi tới database, tiến trình worker bị sập nguồn (`kill -9`). 

Khi Reaper phát hiện worker chết và điều phối worker khác chạy lại job, email bị gửi lần thứ hai tới khách hàng!

### Nguyên nhân gốc rễ
Tương tác với hệ thống bên ngoài (non-transactional external systems) không thể rollback cùng với database transaction.

### Giải pháp kỹ thuật: Transactional Outbox Pattern (ADR-5, G5)
Worker không bao giờ gọi API bên ngoài trực tiếp trong luồng thực thi chính. Thay vào đó, nó ghi sự kiện cần làm vào bảng `outbox` **trong cùng database transaction** với commit kết quả job:

```python
# Worker chỉ emit event vào context
ctx.emit_outbox(
    kind="SEND_EMAIL",
    payload={"to": "user@example.com", "subject": "Order Confirmed"},
    dedup_key=f"order-confirmed-{ctx.job_id}",
)
```

Tiến trình `OutboxDispatcher` chạy ngầm độc lập sẽ quét bảng `outbox` qua `FOR UPDATE SKIP LOCKED` và gửi đi. Nếu worker sập nguồn trước khi commit, toàn bộ transaction bị rollback và không có bất kỳ bản ghi outbox nào được lưu lại.

### Kiểm thử xác thực
- [`tests/integration/test_idempotency_and_outbox.py`](file:///c:/Python/leaseflow/tests/integration/test_idempotency_and_outbox.py): Kiểm chứng worker crash trước commit tạo ra chính xác 0 email ra hệ thống ngoài.

---

## 4. Bug 4: Nghẽn Event Loop Làm Mất Lease Oan Uổng

### Triệu chứng & Tác hại
Một số tác vụ đồng bộ nặng (như parse XML lớn hoặc tính toán hash) khi chạy trong worker làm cho job liên tục bị Reaper thu hồi với lỗi `LEASE_EXPIRED`, mặc dù worker vẫn còn sống và không hề bị crash.

### Nguyên nhân gốc rễ
Python `asyncio` là cơ chế đơn luồng hợp tác (cooperative single-threaded). Nếu một task người dùng chạy code blocking (đồng bộ), event loop bị chiếm dụng hoàn toàn. Task gửi heartbeat của worker không có cơ hội được nhường CPU để gửi request gia hạn lease tới database.

### Giải pháp kỹ thuật: Phân Luồng Sync Handlers vào ThreadPoolExecutor
Tại tầng Worker runtime, tự động kiểm tra chữ ký của handler:
```python
if inspect.iscoroutinefunction(handler):
    result = await handler(ctx)
else:
    # Đẩy tác vụ đồng bộ ra ThreadPoolExecutor riêng biệt
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(self._thread_pool, handler, ctx)
```
Event loop chính luôn thông thoáng để task heartbeat gửi tín hiệu gia hạn đúng chu kỳ `TTL / 3`.

### Kiểm thử xác thực
- [`tests/unit/test_worker_runtime.py::test_worker_handles_sync_function`](file:///c:/Python/leaseflow/tests/unit/test_worker_runtime.py).

---

## 5. Bug 5: `MissingGreenlet` trong SQLAlchemy 2.0 Async với ORM Session Expiration

### Triệu chứng & Tác hại
Trong quá trình triển khai Promoter cập nhật hàng loạt trạng thái `PENDING -> QUEUED`, câu lệnh thực thi update bulk kèm `session.expire_all()`. Khi test case truy cập vào thuộc tính `job.status` sau đó, ứng dụng ném ngoại lệ:
`sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called; can't call await_() here.`

### Nguyên nhân gốc rễ
Sau khi gọi `session.expire_all()`, các đối tượng ORM trong bộ nhớ bị hết hạn. Việc đọc bất kỳ thuộc tính nào (như `job.status`) sau đó sẽ kích hoạt cơ chế lazy-load ngầm định đồng bộ. Trong môi trường SQLAlchemy AsyncIO, lazy-load đồng bộ bị cấm tuyệt đối nếu không nằm trong ngữ cảnh greenlet spawn.

### Giải pháp kỹ thuật
Cấu hình tùy chọn thực thi đồng bộ session tường minh:
```python
stmt = (
    update(JobModel)
    .where(JobModel.status == JobStatus.PENDING, JobModel.run_at <= now)
    .values(status=JobStatus.QUEUED, updated_at=now)
    .execution_options(synchronize_session="fetch")  # Fetch cập nhật trực tiếp state
)
```
Đồng thời loại bỏ lệnh `session.expire_all()` không an toàn.

### Kiểm thử xác thực
- [`tests/unit/test_promoter.py`](file:///c:/Python/leaseflow/tests/unit/test_promoter.py).

---

## 6. Bug 6: Tiến Trình Con Bị Bỏ Rơi (Orphan Process Tree) trên Windows

### Triệu chứng & Tác hại
Khi chạy các kịch bản kiểm thử chaos trên Windows, sau khi gọi `proc.kill()`, cổng database vẫn bị chiếm dụng và các worker vẫn tiếp tục claim job ngầm dưới background, làm sai lệch toàn bộ kết quả kiểm thử.

### Nguyên nhân gốc rễ
Trên môi trường Windows, package manager `uv` bọc lệnh chạy `python` qua một wrapper shim binary. Khi gọi `kill()` trên PID của subprocess, chỉ có tiến trình wrapper bị tiêu diệt, còn tiến trình con thực tế chứa mã nguồn Python vẫn tiếp tục chạy độc lập trong hệ điều hành.

### Giải pháp kỹ thuật: Duyệt Đệ Quy Cây Tiến Trình qua `psutil`
Xây dựng lớp điều phối [`WorkerProcess`](file:///c:/Python/leaseflow/tests/chaos/harness.py) sử dụng `psutil` để truy vết và tiêu diệt triệt để toàn bộ cây tiến trình con:
```python
def kill(self) -> None:
    if self._psutil_proc and self.is_alive():
        for child in self._psutil_proc.children(recursive=True):
            try:
                child.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        self._psutil_proc.kill()
```

### Kiểm thử xác thực
- [`tests/chaos/test_chaos_worker_kill.py`](file:///c:/Python/leaseflow/tests/chaos/test_chaos_worker_kill.py).
