#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
name="zbsi71-skill-$(date +%s)-$$"
owner="ZBSI-71-$name"
cid=""
cleanup() {
  if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null || true)" == "$owner" ]]; then
    docker rm -f "$cid" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
cid=$(docker run -d --name "$name" --label "multica.test.owner=$owner" --memory=768m --cpus=1 \
  -e POSTGRES_USER=zbsi71 -e POSTGRES_PASSWORD=synthetic_zbsi71 \
  -e POSTGRES_DB=multica_zbsi71 -p 127.0.0.1::5432 postgres:16)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]]
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U zbsi71 -d multica_zbsi71 >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$cid" pg_isready -h 127.0.0.1 -U zbsi71 -d multica_zbsi71
url="postgres://zbsi71:synthetic_zbsi71@127.0.0.1:$port/multica_zbsi71"
export RUST_DATABASE_URL="$url" RUST_API_TEST_DATABASE_URL="$url"
export RUST_DEV_DATABASE_CONFIRM=multica_zbsi71 RUST_MIGRATIONS_ROOT="$root"
cargo=/home/zbstech/.cargo/bin/cargo
"$cargo" test --manifest-path "$root/Cargo.toml" --offline --locked --lib runtime_skills_sprint -- --nocapture
"$cargo" run --manifest-path "$root/Cargo.toml" --offline --locked -q -- migrate-up
"$cargo" test --manifest-path "$root/Cargo.toml" --offline --locked --test runtime_skills_sprint -- --ignored --nocapture --test-threads=1
"$cargo" test --manifest-path "$root/Cargo.toml" --offline --locked --test runtime_skills_sprint_bundles -- --ignored --nocapture --test-threads=1
