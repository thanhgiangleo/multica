## Throughput Go vs Rust — cùng CPU budget, cùng Postgres

Đo theo yêu cầu "đo throughput, rồi benchmark". Trạng thái issue giữ `in_progress`. Không commit/push/PR, không cutover, không production, không gọi provider.

### 0. Một bug trong harness, bắt được trước khi báo cáo

Lần chạy đầu in ra `server_cpu_pct = 259.2%` trên trần 2 CPU — bất khả thi về mặt vật lý. Nguyên nhân: tử số `usage_usec` đo qua cả `warmup + duration` (8 s) nhưng mẫu số chỉ là `duration` (6 s). Đã sửa: thêm `cpu_window = warmup + duration`, ghi `cpu_window_s` vào từng row. `zbsi60-throughput-1.json` là kết quả của bản lỗi — giữ lại để đối chiếu nhưng **không dùng làm số**. Toàn bộ số dưới đây lấy từ `zbsi60-throughput-2.json`, CPU server cao nhất 195.9% < 200% trần.

### 1. Cấu hình đo (giống nhau cho hai backend)

- Postgres `postgres:17` riêng, `1536m/2cpu`, chỉ publish loopback — là hạ tầng, không phải backend được đo.
- Mỗi backend `--memory=1024m --cpus=2` (đúng bằng nhau), cùng DB, cùng fixture.
- Oracle Go SHA-256 `bafb4054…e9e00fe36`; Go migrate `076f9f90…60ad7d4e`; Rust release binary SHA-256 `c0a55949…ec6a76fe` (26,224,296 B).
- Trước khi đo tải, **4/4 case certified pass contract** trên từng backend; 32/32 row sau đó `failures = 0`.
- Client: Python đa tiến trình (`min(c,4)`) × đa luồng, keep-alive, 1 connection/thread; mỗi mức 2 s warmup + 6 s đo.

### 2. Kết quả

| Case | c | Go rps | Rust rps | Rust/Go | Go p95 ms | Rust p95 ms | Rust p99 ms | Go CPU% | Rust CPU% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `read-issue` | 1 | 2606 | 752 | 0.29 | 0.72 | 1.99 | 2.9 | 44 | 17 |
| `read-issue` | 8 | 11982 | 2093 | 0.17 | 0.97 | 2.23 | 56.6 | 176 | 61 |
| `read-issue` | 32 | 11542 | 2063 | 0.18 | 4.21 | 63.52 | 64.4 | 156 | 63 |
| `read-issue` | 64 | 10144 | 2075 | 0.20 | 20.98 | 70.67 | 71.9 | 158 | 64 |
| `read-agent` | 1 | 3446 | 924 | 0.27 | 0.48 | 1.76 | 2.3 | 43 | 22 |
| `read-agent` | 8 | 14218 | 3375 | 0.24 | 0.84 | 1.67 | 47.8 | 179 | 80 |
| `read-agent` | 32 | 13314 | 3542 | 0.27 | 3.92 | 53.24 | 54.3 | 164 | 83 |
| `read-agent` | 64 | 13034 | 3598 | 0.28 | 8.54 | 58.05 | 58.9 | 166 | 84 |
| `telegram-list-disabled` | 1 | 7026 | 434 | 0.06 | 0.26 | 3.97 | 7.7 | 50 | 7 |
| `telegram-list-disabled` | 8 | 28933 | 1846 | 0.06 | 0.46 | 7.24 | 11.0 | 182 | 24 |
| `telegram-list-disabled` | 32 | 32598 | 2239 | 0.07 | 2.00 | 18.43 | 158.1 | 196 | 30 |
| `telegram-list-disabled` | 64 | 31171 | 2320 | 0.07 | 3.99 | 32.78 | 232.1 | 194 | 30 |
| `missing-session` | 1 | 6197 | 2536 | 0.41 | 0.27 | 0.81 | 1.2 | 47 | 26 |
| `missing-session` | 8 | 23568 | 9382 | 0.40 | 0.54 | 0.62 | 1.3 | 181 | 98 |
| `missing-session` | 32 | 24618 | 9419 | 0.38 | 2.50 | 2.37 | 46.7 | 190 | 98 |
| `missing-session` | 64 | 25157 | 9512 | 0.38 | 4.74 | 46.88 | 48.7 | 195 | 100 |

### 3. Đọc số

1. **Rust thua ở mọi case, mọi mức**: tỷ lệ rps Rust/Go 0.06–0.41, median 0.25. Không có một case nào Rust thắng. Nên câu hỏi trước đây — Rust có thắng Go không — có câu trả lời rõ: **không**, và không chỉ ở latency mà cả ở capacity.
2. **Rust bão hoà từ c=8 rồi đứng yên** khi lên 32/64: `read-issue` 2093 → 2075, `read-agent` 3375 → 3598, `telegram` 1846 → 2320, `missing-session` 9382 → 9512. Go tăng 4.5–5× từ c=1 lên c=8 rồi giữ mức cao.
3. **Rust không nghẽn CPU**: 7–100% trên trần 200%. Go chạm 156–196%, tức Go đang sát trần CPU được cấp. Vậy khoảng cách **không phải** do thiếu CPU budget — Rust còn dư CPU mà vẫn không đẩy thêm được request.
4. **CPU cho mỗi request** (`server_cpu_s / n`): Go 0.083–0.223 mJ, Rust 0.134–0.410 mJ ⇒ Rust tốn **1.36–2.4×** CPU hơn cho cùng một request, kể cả ở c=1 nơi chưa có tranh chấp.
5. **Tail là chỗ đáng lo hơn rps trung bình.** `read-issue` c=8: Rust p95 2.23 ms nhưng p99 **56.6 ms** trong khi Go p99 1.21 ms — phân bố hai đỉnh, khoảng 1% request kẹt 50–70 ms trong khi đa số vẫn nhanh. Ở c=32/64 Rust p95 nhảy lên 53–70 ms; `telegram-list-disabled` p99 158–232 ms.
6. **Nghi phạm số 1 (giả thuyết, chưa chứng minh)**: plateau bắt đầu đúng ở c=8, khớp `deadpool` pool `max_size = 8` trong `src/main.rs`. Với c>8, request xếp hàng chờ connection; p99 ~50 ms cũng khớp hình dạng một ngưỡng chờ. Phải đo riêng (tăng pool size, hoặc đếm thời gian chờ acquire) trước khi kết luận — tôi chưa làm và không nhận đây là nguyên nhân.
7. **Case tệ nhất — `telegram-list-disabled`**: 0.06–0.07× (15–25× chậm hơn), Go 7.0k–32.6k rps so với Rust 434–2320 rps. Body chỉ 66 byte nên gần như toàn bộ chênh lệch là chi phí cố định của Rust, không phải dữ liệu. Ngược lại `missing-session` (35 byte, 404) là case Rust khá nhất: 0.38–0.41×.

### 4. Giới hạn của phép đo — không được bỏ qua khi trích dẫn

- Closed loop từng mức: p95 ở c=32/64 là latency mà client ở mức đó thấy, có coordinated omission, không phải latency lý tưởng open-loop.
- Generator là Python. Ở các row của Go, client dùng tới **278% CPU** (nhiều process) ⇒ con số Go ở c=32/64 **có thể bị client chặn**, nghĩa là ưu thế thật của Go có thể còn lớn hơn, không nhỏ hơn. Tôi không kết luận Go đã đạt hạn thật của nó.
- Toàn bộ 12 row Rust ở c≥8 bị đánh dấu `client_bound` (server còn >40% trần CPU rảnh): generator không ép được server Rust. Nhưng trần vẫn nằm **bên trong** server Rust, không phải ở client — client chỉ dùng 26–82% một core.
- Fixture nhỏ đã certified ⇒ đo chi phí cố định mỗi request, không đo serialize payload lớn.
- Loopback, một host, không TLS/proxy/network thật; không suy p95 end-to-end từ p95 từng tầng.

### 5. Một điểm chưa giải thích

`read-issue`: body trung bình Go **647 B**, Rust **649 B**, ổn định qua cả 4 mức. `contract_sha256` của differential vẫn khớp (`differences: []`) nên lệch nằm trong phần đã chuẩn hoá, nhưng đây là 2 byte lệch trên một endpoint mà owner yêu cầu byte-identical. Cần bóc đúng field trước khi tuyên bố tương đương byte — tôi chưa làm.

### 6. Artifact

- `server-rust/zbsi60_throughput.py` — harness (đã sửa bug CPU window)
- `server-rust/zbsi60-throughput-2.json` + `zbsi60-throughput-2.log` — số dùng trong comment này
- `server-rust/zbsi60-throughput-1.json` + `.log` — bản lỗi mẫu số, chỉ để đối chiếu

### 7. Vẫn chưa làm / vẫn cấm

- Chưa commit/push/PR, chưa cutover, chưa production, không đọc `.env`/DB thật, không gọi provider, không real-agent smoke.
- Chưa nối `with_local_issue_event_dispatcher` + `with_issue_comment_runtime`.
- Issue giữ `in_progress`: đây là benchmark, chưa phải bằng chứng parity.