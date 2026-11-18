#!/usr/bin/env bash
# Disposable PostgreSQL for the workflow-handoff preview contract test.
# Run-owned: a unique label makes cleanup refuse to touch any other container.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
owner="zbsi100-handoff-$(date +%s)-$$"
cid=""
cleanup() {
  if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null || true)" == "$owner" ]]; then
    docker rm -f "$cid" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
cid=$(docker run -d --label "multica.test.owner=$owner" --memory=1024m --cpus=2 \
  -e POSTGRES_USER=zbsi100 -e POSTGRES_PASSWORD=synthetic_zbsi100 \
  -e POSTGRES_DB=multica_zbsi100_handoff -p 127.0.0.1::5432 postgres:17)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]]
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U zbsi100 -d multica_zbsi100_handoff >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$cid" pg_isready -h 127.0.0.1 -U zbsi100 -d multica_zbsi100_handoff
url="postgres://zbsi100:synthetic_zbsi100@127.0.0.1:$port/multica_zbsi100_handoff"
export RUSTUP_TOOLCHAIN=1.95.0 RUST_MIGRATIONS_ROOT="$root"
export RUST_DATABASE_URL="$url" RUST_API_TEST_DATABASE_URL="$url"
export RUST_DEV_DATABASE_CONFIRM=multica_zbsi100_handoff
cargo=/home/zbstech/.cargo/bin/cargo
"$cargo" run --manifest-path "$root/Cargo.toml" --offline --locked -q -- migrate-up
"$cargo" test --manifest-path "$root/Cargo.toml" --offline --locked \
  --test p0_workflow_handoff_db -- --ignored --nocapture --test-threads=1
