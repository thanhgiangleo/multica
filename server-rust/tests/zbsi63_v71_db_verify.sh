#!/usr/bin/env bash
# ZBSI-63 combined DB gate: each owned suite keeps its own disposable database
# name assertion, and every container is created, verified and removed by this
# process only.
set -euo pipefail
cd "$(dirname "$0")/.."
export RUSTUP_TOOLCHAIN=1.95.0
cargo=/home/zbstech/.cargo/bin/cargo

run_suite() {
  local db="$1" suite="$2"
  local owner="zbsi63-v71-$suite-$(date +%s)-$$"
  local cid=""
  cleanup() {
    if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null)" == "$owner" ]]; then
      docker rm -f "$cid" >/dev/null
    fi
  }
  trap cleanup RETURN
  cid=$(docker run -d --label "multica.test.owner=$owner" --label "multica.test.issue=ZBSI-63" \
    --memory=768m --cpus=2 -e POSTGRES_USER=zbsi63 -e POSTGRES_PASSWORD=synthetic_zbsi63 \
    -e POSTGRES_DB="$db" -p 127.0.0.1::5432 postgres:16)
  local port
  port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
  [[ "$port" != 5432 ]] || { printf 'Refusing host 5432\n' >&2; exit 1; }
  for _ in $(seq 1 60); do
    docker exec "$cid" pg_isready -h 127.0.0.1 -U zbsi63 -d "$db" >/dev/null 2>&1 && break
    sleep 1
  done
  local url="postgres://zbsi63:synthetic_zbsi63@127.0.0.1:$port/$db"
  RUST_DATABASE_URL="$url" RUST_DEV_DATABASE_CONFIRM="$db" \
    RUST_MIGRATIONS_ROOT="$PWD" "$cargo" run --locked --offline -q -- migrate-up
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --offline --locked \
    --test "$suite" -- --ignored --nocapture --test-threads=1
  cleanup
  trap - RETURN
}

run_suite multica_zbsi65 account_catalog_write_views
run_suite multica_zbsi76 files_ops_sprint
