#!/usr/bin/env bash
# Run only through sprint-gate.py --kind db. No shared target or database access.
#
# End-to-end DB gate for the comment-arm subscriber listener (ZBSI-60): one
# owned disposable PostgreSQL 16 on a random non-5432 loopback port, migrated
# with this tree's migrations, then the ignored test that publishes a real
# `comment:created` through the process bus and checks the subscriber row plus
# the `subscriber:added` fanout.
set -euo pipefail
cd "$(dirname "$0")"
owner="comment-subscriber-zbsi60-$(date +%s)-$$"
cid=""
cleanup() {
  if [[ -n "$cid" ]]; then
    if [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null)" == "$owner" ]]; then
      docker rm -f "$cid" >/dev/null
      printf 'Removed owned test PostgreSQL %s\n' "$cid"
    else
      printf 'Refusing cleanup: ownership label mismatch for %s\n' "$cid" >&2
      return 1
    fi
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
cid=$(docker run -d --label "multica.test.owner=$owner" --memory=768m --cpus=2 \
  -e POSTGRES_USER=comment_subscriber -e POSTGRES_PASSWORD=synthetic_comment_subscriber \
  -e POSTGRES_DB=multica_zbsi60 -p 127.0.0.1::5432 postgres:16)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]] || { printf 'Refusing invalid or shared port\n' >&2; exit 1; }
ready=false
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U comment_subscriber -d multica_zbsi60 >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { printf 'PostgreSQL readiness timeout\n' >&2; exit 1; }
export RUSTUP_TOOLCHAIN=1.95.0
cargo=/home/zbstech/.cargo/bin/cargo
url="postgres://comment_subscriber:synthetic_comment_subscriber@127.0.0.1:$port/multica_zbsi60"
printf 'Owned disposable PostgreSQL 16 ready on loopback:%s\n' "$port"
RUST_DATABASE_URL="$url" RUST_DEV_DATABASE_CONFIRM=multica_zbsi60 RUST_MIGRATIONS_ROOT="$PWD" \
  "$cargo" run --locked --offline -q -- migrate-up
RUST_API_TEST_DATABASE_URL="$url" \
  "$cargo" test --locked --offline --lib comment_subscriber_listener -- --ignored --nocapture --test-threads=1