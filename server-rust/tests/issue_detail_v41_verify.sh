#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="${HOME}/.cargo/bin:${PATH}" RUSTUP_TOOLCHAIN=1.95.0
name="zbsi63-detail-$(date +%s)-$$"
docker run --pull never -d --name "$name" --label multica.issue=ZBSI-63 --memory 384m --cpus 1 \
  -e POSTGRES_PASSWORD=synthetic_origin -e POSTGRES_DB=multica_zbsi69 \
  -p 127.0.0.1::5432 postgres:16 >/dev/null
cleanup() {
  owner=$(docker inspect "$name" --format '{{index .Config.Labels "multica.issue"}}' 2>/dev/null || true)
  if [[ "$owner" == ZBSI-63 ]]; then docker rm -f "$name" >/dev/null; fi
}
trap cleanup EXIT
for _ in $(seq 1 45); do
  if docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d multica_zbsi69 >/dev/null; then break; fi
  sleep 1
done
docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d multica_zbsi69
port=$(docker inspect "$name" --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}')
[[ "$port" != 5432 ]]
export RUST_API_TEST_DATABASE_URL="host=127.0.0.1 port=$port user=postgres password=synthetic_origin dbname=multica_zbsi69"
RUST_DATABASE_URL="$RUST_API_TEST_DATABASE_URL" RUST_DEV_DATABASE_CONFIRM=multica_zbsi69 RUST_MIGRATIONS_ROOT="$PWD" cargo run --offline --locked -- migrate-up
cargo test --offline --locked --test queries_sprint_issue_detail -- --ignored --nocapture
