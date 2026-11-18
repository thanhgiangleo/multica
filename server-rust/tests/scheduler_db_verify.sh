#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
owner="zbsi-scheduler-$(date +%s)-$$"
cid=""
cleanup() {
  if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null || true)" == "$owner" ]]; then
    docker rm -f "$cid" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
cid=$(docker run -d --label "multica.test.owner=$owner" --memory=1024m --cpus=2 \
  -e POSTGRES_USER=scheduler -e POSTGRES_PASSWORD=synthetic_scheduler \
  -e POSTGRES_DB=multica_scheduler_contract -p 127.0.0.1::5432 postgres:17)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]]
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U scheduler -d multica_scheduler_contract >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$cid" pg_isready -h 127.0.0.1 -U scheduler -d multica_scheduler_contract
url="postgres://scheduler:synthetic_scheduler@127.0.0.1:$port/multica_scheduler_contract"
export RUSTUP_TOOLCHAIN=1.95.0
export RUST_API_TEST_DATABASE_URL="$url"
cargo=/home/zbstech/.cargo/bin/cargo
"$cargo" test --manifest-path "$root/Cargo.toml" --offline --locked \
  --test scheduler_db -- --ignored --nocapture --test-threads=1
