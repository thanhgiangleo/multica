## Combined gate xanh + differential + latency (chưa commit, chưa cutover)

Trạng thái issue giữ `in_progress`. Không có commit/push/PR, không đụng production, không gọi provider.

### 1. Năm leg của `sprint-gate.py` — tất cả exit 0

Chạy trên cây hiện tại, Rust 1.95.0, log nằm trong `server-rust/`.

| Leg | Exit | Log | Bằng chứng |
|---|---|---|---|
| `cargo fmt --all -- --check` | **0** | `zbsi60-fmt-check-2.log` | file log 0 byte (không còn diff) |
| `cargo clippy --lib --tests -- -D warnings` | **0** | `zbsi60-clippy-11-lib-tests.log` | `Finished \`dev\` profile ... in 1m 30s` |
| `cargo clippy --all-targets -- -D warnings` | **0** | `zbsi60-clippy-12-all-targets.log` | `Finished \`dev\` profile ... in 4.74s` |
| `cargo test --lib` | **0** | `zbsi60-test-lib-1.log` | `test result: ok. 507 passed; 0 failed; 4 ignored` |
| `cargo test --tests` | **0** | `zbsi60-test-tests-1.log` | 137 suite → `524 passed, 0 failed, 392 ignored` |

Kiểm tra độ tươi: `find src tests Cargo.toml -type f -newer zbsi60-fmt-check-2.log` không trả về file nào ⇒ cả 5 leg còn hiệu lực trên đúng cây nguồn hiện tại.

### 2. Phạm vi fmt/clippy đã sửa cơ học

**fmt — 9 file.** `zbsi60-fmt-6.log` (lần áp `cargo fmt --all`) liệt kê 8 file:

- `src/api/catalog.rs`
- `src/api/p0_daemon_outcomes.rs`
- `src/api/p0_daemon_ws.rs`
- `tests/issue_wakeup_scheduler.rs`
- `tests/p0_daemon_claim_db.rs`
- `tests/queries_sprint_limit_usage.rs`
- `tests/runtime_sweeper_contract.rs`
- `tests/scheduler_db.rs`

Cộng thêm `src/api/daemon_claim_pure.rs` do `zbsi60-fmt-check-1.log` báo và sửa tay ⇒ **9 file**.

**clippy — 7 file theo log của lane này** (`zbsi60-clippy-[0-9]*.log`, dedupe theo dòng `-->`):

- `src/api/daemon_claim_pure.rs`
- `src/api/p0_tasks.rs`
- `src/api/runtime_chat.rs`
- `tests/issue_wakeup_put_contract.rs`
- `tests/p0_squad_evaluation_db.rs`
- `tests/queries_sprint_limit_usage.rs`
- `tests/system_wakeup_put_contract.rs`

Con số "19 file clippy" của owner **không tái lập được từ artifact còn giữ**: log clippy của lane này chỉ ra 7 file. Tập 24 file (20 src + 4 tests) chỉ xuất hiện khi gộp cả log `zbsi60-listener-clippy-*.log` của lane comment-subscriber đang chạy song song — số đó không phải phần lane này sửa. Tôi báo đúng những gì log chứng minh, không gán thêm.

Các sửa tiêu biểu: `collapsible_if` → `if let ... && ...` (Rust 2024 let-chains) trong `daemon_claim_response.rs` / `p0_tasks.rs`; `bool_assert_comparison` → `assert!(x)` / `assert!(!x)` trong 3 file test; `NeedlessRangeLoop`/borrow trong `runtime_chat.rs`. Riêng `p0_tasks.rs` giữ `&*tx` (không đổi thành `&tx`) để `deadpool_postgres::Transaction` deref xuống `tokio_postgres::Transaction` cho `GenericClient`.

### 3. Cây làm việc có cả việc đang bay của lane khác

Dù không commit, cần ghi rõ: cây mà gate xanh ở trên phủ **không chỉ** thay đổi của lane này.

- `src/api.rs`: thêm `pub mod comment_subscriber_listener;` (lane comment-subscriber, file `src/api/comment_subscriber_listener.rs` còn untracked) **và** đổi `mod issue_write_create_wakeup;` → `pub(crate) mod issue_write_create_wakeup;`.
- `src/scheduler_issue_wakeup.rs`: bỏ `#[path = "api/issue_write_cron.rs"] mod cron;` → `use crate::api::issue_write_create_wakeup::cron;`.

Không có thay đổi nào trong số này thuộc phần fmt/clippy cơ học, và tôi không nhận là tác giả của chúng.

### 4. Differential Go↔Rust trên binary build từ cây hiện tại

`run_merge_diff.py` hard-code `RUST_BIN` vào `target-zbsi-100` (artifact cũ hơn source). Để evidence chứng đúng cây hiện tại, tôi thêm launcher `zbsi60_diff_fresh_bin.py` chỉ override module constant đó sang `target-zbsi-60/debug/multica-server`; harness gốc không bị sửa.

- Oracle SHA-256 `bafb4054...e9e00fe36`; Go migrate SHA-256 `076f9f90...60ad7d4e`
- Binary Rust SHA-256 `36b0d1dc9ed9cc20fa75bcbf1131454a4592cea72e4b6d2def15c07bc0e5efc0`
- Stack loopback dùng-một-lần, DB mới: pg 43627 / go 42885 / rust 47765
- 5 group đều `exit 0`: base-all 30, read-supplemental 2, workspace-isolation 2, P0 6, P1 47 = **87 case/backend**
- `differences: []`, `unexecuted: []`, candidate failure 0 ở cả 5 group
- `route probe rows: 954` — chỉ là số dòng probe, **không** tính là coverage

### 5. Latency, n=100 mỗi case GET

Cùng stack cô lập ở trên, `zbsi60_latency.py` (samples 100, warmup 5, closed-loop concurrency 1, cap 20 rps) → `zbsi60-latency-2.json`.

- **40 case GET ghép cặp**, mỗi backend `n = 100`, tổng `failures = 0`, `below_100: []` (không case nào dưới 100 mẫu)
- Rust p95 tuyệt đối: **5.06 – 15.20 ms**
- Tỷ lệ p95 Rust/Go: min 0.89, median 1.25, max 2.45
- Xấu nhất so với Go: `invalid-page-limit` 6.21 → 15.20 ms (×2.45); `wecom-list-disabled` ×1.84; `missing-session` ×1.81; `dingtalk-list-disabled` ×1.81; `telegram-list-disabled` ×1.81
- Tốt nhất: `anonymous-pending` ×0.89, `anonymous-agents` ×0.98, `anonymous-issues` ×1.00
- Cảnh báo kèm trong report: có overhead network/client, chỉ đo case thành công, closed loop không phải saturation, có coordinated omission

### 6. Vẫn chưa làm / vẫn cấm

- Chưa nối `with_local_issue_event_dispatcher` + `with_issue_comment_runtime` (và không dùng `with_issue_create_runtime`).
- Không commit/push/PR, không cutover, không production, không đọc `.env`/DB thật, không gọi provider, không real-agent smoke.
- Bốn entry in-flight có sẵn (`run_merge_diff.py`, `src/lib.rs`, `src/scheduler_issue_wakeup.rs`, `src/issue_wakeup_dispatch.rs`) không bị gộp vào bất kỳ thay đổi nào của lane này.