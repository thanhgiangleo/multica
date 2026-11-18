#!/usr/bin/env bash
# Run only via sprint-gate.py --issue ZBSI-75 --kind db.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${CARGO_TARGET_DIR:?Run through sprint-gate.py}"
[[ "$CARGO_TARGET_DIR" == /data/rust-build/zbstech/.multica-rust-sprint-20261102/target-zbsi-75 ]]
[[ "${CARGO_BUILD_JOBS:-}" == 1 ]]
export RUSTUP_TOOLCHAIN=1.95.0
cargo_bin="${HOME}/.cargo/bin/cargo"
"$cargo_bin" test --offline --locked --lib automation_sprint
name="zbsi75-automation-$(date +%s)-$$"
run="6a35a66815c5-v5-$$"
cleanup() {
  local owner run_owner
  owner=$(docker inspect "$name" --format '{{index .Config.Labels "multica.issue"}}' 2>/dev/null || true)
  run_owner=$(docker inspect "$name" --format '{{index .Config.Labels "multica.run"}}' 2>/dev/null || true)
  if [[ "$owner" == ZBSI-75 && "$run_owner" == "$run" ]]; then
    docker rm -f "$name" >/dev/null
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker run --pull never -d --name "$name" --label multica.issue=ZBSI-75 --label "multica.run=$run" \
  --memory 384m --cpus 1 -e POSTGRES_PASSWORD=synthetic_zbsi75 -e POSTGRES_DB=multica_zbsi75 \
  -p 127.0.0.1::5432 postgres:16
for ((i=0;i<45;i++)); do
  if docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d multica_zbsi75 >/dev/null; then break; fi
  sleep 1
done
docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d multica_zbsi75
port=$(docker inspect "$name" --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}')
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]]
export RUST_API_TEST_DATABASE_URL="host=127.0.0.1 port=$port user=postgres password=synthetic_zbsi75 dbname=multica_zbsi75"
"$cargo_bin" test --offline --locked --test automation_sprint_definitions -- --ignored --nocapture
