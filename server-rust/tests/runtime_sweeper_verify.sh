#!/usr/bin/env bash
# Run only through sprint-gate.py --issue ZBSI-100 --kind db.
set -euo pipefail
: "${CARGO_TARGET_DIR:?run through sprint-gate.py}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
owner="zbsi100-runtime-$(date +%s)-$$"
cid=""
cleanup() {
  if [[ -n "$cid" ]] && [[ "$(docker inspect -f '{{index .Config.Labels "multica.test.owner"}}' "$cid" 2>/dev/null || true)" == "$owner" ]]; then
    docker rm -f "$cid" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM
cid=$(docker run -d --label "multica.test.owner=$owner" --memory=1024m --cpus=2 \
  -e POSTGRES_USER=runtime -e POSTGRES_PASSWORD=synthetic_runtime \
  -e POSTGRES_DB=multica_runtime_sweep_contract -p 127.0.0.1::5432 postgres:17)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]]
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U runtime -d multica_runtime_sweep_contract >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$cid" pg_isready -h 127.0.0.1 -U runtime -d multica_runtime_sweep_contract
export RUSTUP_TOOLCHAIN=1.95.0
export RUST_API_TEST_DATABASE_URL="postgres://runtime:synthetic_runtime@127.0.0.1:$port/multica_runtime_sweep_contract"
/home/zbstech/.cargo/bin/cargo test --manifest-path "$root/Cargo.toml" --offline --locked \
  --test runtime_sweeper_contract -- --ignored --nocapture --test-threads=1
