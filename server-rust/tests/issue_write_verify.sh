#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="${HOME}/.cargo/bin:${PATH}" RUSTUP_TOOLCHAIN=1.95.0
if [[ "${1:-}" != --gate-held ]]; then
  exec python3 tests/issue_write_sprint_gate.py --issue ZBSI-66 --kind db \
    --log "issue_write_verify-$(date +%s)-$$.log" --wait-seconds 120 --timeout-seconds 480 \
    -- bash tests/issue_write_verify.sh --gate-held
fi
[[ "${CARGO_BUILD_JOBS:-}" == 1 ]]
[[ "${CARGO_TARGET_DIR:-}" == /data/rust-build/zbstech/.multica-rust-sprint-20261102/target-zbsi-66 ]]
cargo fmt --all -- --check
cargo test --lib --locked --offline
cargo clippy --lib --test issue_write_metadata --test issue_write_comment --test issue_write_update_subscriber --test issue_write_create --test issue_write_delete_move --locked --offline -- -D warnings
name="zbsi66-structure-$(date +%s)-$$"
docker run -d --name "$name" --label multica.issue=ZBSI-66 --memory 384m --cpus 1 \
  -e POSTGRES_PASSWORD=synthetic_zbsi66 -e POSTGRES_DB=multica_zbsi66 \
  -p 127.0.0.1::5432 postgres:16
cleanup() {
  owner=$(docker inspect "$name" --format '{{index .Config.Labels "multica.issue"}}' 2>/dev/null || true)
  if [[ "$owner" == ZBSI-66 ]]; then docker rm -f "$name" >/dev/null; fi
}
trap cleanup EXIT
for _ in $(seq 1 45); do
  if docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d multica_zbsi66 >/dev/null; then break; fi
  sleep 1
done
docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d multica_zbsi66
port=$(docker inspect "$name" --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}')
[[ "$port" != 5432 ]]
export RUST_API_TEST_DATABASE_URL="host=127.0.0.1 port=$port user=postgres password=synthetic_zbsi66 dbname=multica_zbsi66"
cargo test --test issue_write_metadata --test issue_write_comment --test issue_write_update_subscriber --test issue_write_create --test issue_write_delete_move --locked --offline --no-fail-fast -- --ignored --nocapture
