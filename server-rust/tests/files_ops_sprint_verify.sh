#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
owner="zbsi76-files-$(date +%s)-$$"
cid=""
cleanup() {
  if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null)" == "$owner" ]]; then
    docker rm -f "$cid" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
cid=$(docker run -d --label "multica.test.owner=$owner" --label "multica.test.issue=ZBSI-76" \
  --memory=768m --cpus=2 -e POSTGRES_USER=filesops -e POSTGRES_PASSWORD=synthetic_filesops \
  -e POSTGRES_DB=multica_zbsi76 -p 127.0.0.1::5432 postgres:16)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" != 5432 ]] || { printf 'Refusing host 5432\n' >&2; exit 1; }
for _ in $(seq 1 60); do
  docker exec "$cid" pg_isready -h 127.0.0.1 -U filesops -d multica_zbsi76 >/dev/null 2>&1 && break
  sleep 1
done
# Retain target/jobs/debug limits inherited from sprint-gate.py.
export RUSTUP_TOOLCHAIN=1.95.0
cargo=/home/zbstech/.cargo/bin/cargo
url="postgres://filesops:synthetic_filesops@127.0.0.1:$port/multica_zbsi76"
RUST_DATABASE_URL="$url" RUST_DEV_DATABASE_CONFIRM=multica_zbsi76 \
  RUST_MIGRATIONS_ROOT="$PWD" "$cargo" run --locked --offline -q -- migrate-up
RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --offline --locked --test files_ops_sprint -- --ignored --nocapture --test-threads=1
