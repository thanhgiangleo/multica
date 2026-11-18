#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
owner="zbsi-wakeup-$(date +%s)-$$"
cid=""
cleanup() {
  if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null || true)" == "$owner" ]]; then
    docker rm -f "$cid" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
cid=$(docker run -d --label "multica.test.owner=$owner" --memory=1024m --cpus=2 \
  -e POSTGRES_USER=wakeup -e POSTGRES_PASSWORD=synthetic_wakeup \
  -e POSTGRES_DB=multica_wakeup_scheduler_contract -p 127.0.0.1::5432 postgres:17)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]]
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U wakeup -d multica_wakeup_scheduler_contract >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$cid" pg_isready -h 127.0.0.1 -U wakeup -d multica_wakeup_scheduler_contract
export RUSTUP_TOOLCHAIN=1.95.0
export RUST_API_TEST_DATABASE_URL="postgres://wakeup:synthetic_wakeup@127.0.0.1:$port/multica_wakeup_scheduler_contract"
"/home/zbstech/.cargo/bin/cargo" test --manifest-path "$root/Cargo.toml" --offline --locked \
  --test issue_wakeup_scheduler -- --ignored --nocapture --test-threads=1
