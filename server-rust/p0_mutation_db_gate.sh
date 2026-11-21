#!/usr/bin/env bash
# Run only through sprint-gate.py --kind db. No shared target or database access.
#
# P0 mutation + event-visibility gate for ZBSI-60. Usage:
#
#   bash p0_mutation_db_gate.sh agent-task   # multica_zbsi67
#   bash p0_mutation_db_gate.sh ws           # multica_zbsi66
#
# One owned disposable PostgreSQL 17 on a random non-5432 loopback port, the
# needed throwaway database(s) migrated from this tree, then the ignored tests
# that drive the P0 mutations over real HTTP/WS:
#
#   agent-task  agents archive / cancel-tasks / task page, task cancel + reads
#   ws          agent env PUT, task cancel and archive/cancel-tasks delivered to
#               the member socket only, never to a foreign workspace
#
# The point is that these routes answer from wired workspace event buses
# instead of the empty-bus 503, and that the resulting events respect workspace
# membership.
set -euo pipefail
cd "$(dirname "$0")"
mode="${1:-}"
case "$mode" in
  agent-task) dbs=(multica_zbsi67) ;;
  ws) dbs=(multica_zbsi66) ;;
  *) printf 'usage: %s agent-task|ws\n' "$0" >&2; exit 2 ;;
esac
owner="p0-mutations-zbsi60-$(date +%s)-$$"
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
cid=$(docker run -d --label "multica.test.owner=$owner" --memory=1024m --cpus=2 \
  -e POSTGRES_USER=p0_mutations -e POSTGRES_PASSWORD=synthetic_p0_mutations \
  -e POSTGRES_DB=postgres -p 127.0.0.1::5432 postgres:17)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]] || { printf 'Refusing invalid or shared port\n' >&2; exit 1; }
ready=false
for _ in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U p0_mutations -d postgres >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { printf 'PostgreSQL readiness timeout\n' >&2; exit 1; }
export RUSTUP_TOOLCHAIN=1.95.0
cargo=/home/zbstech/.cargo/bin/cargo
printf 'Owned disposable PostgreSQL 17 ready on loopback:%s\n' "$port"
for db in "${dbs[@]}"; do
  docker exec "$cid" psql -h 127.0.0.1 -U p0_mutations -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE $db" >/dev/null
  url="postgres://p0_mutations:synthetic_p0_mutations@127.0.0.1:$port/$db"
  printf 'Migrating %s\n' "$db"
  RUST_DATABASE_URL="$url" RUST_DEV_DATABASE_CONFIRM="$db" RUST_MIGRATIONS_ROOT="$PWD" \
    "$cargo" run --locked --offline -q -- migrate-up
done
rc=0
run() {
  local label=$1 db=$2 target=$3
  printf '\n=== %s ===\n' "$label"
  if RUST_API_TEST_DATABASE_URL="postgres://p0_mutations:synthetic_p0_mutations@127.0.0.1:$port/$db" \
    "$cargo" test --locked --offline --test "$target" -- --ignored --nocapture --test-threads=1; then
    printf '%s: PASS\n' "$label"
  else
    printf '%s: FAIL\n' "$label" >&2
    rc=1
  fi
}
if [[ "$mode" == agent-task ]]; then
  run agent-archive-cancel multica_zbsi67 p0_agent_routes_db
  run task-cancel-and-reads multica_zbsi67 p0_task_routes_db
else
  run ws-env-task-archive multica_zbsi66 issue_event_bridge_ws
fi
exit "$rc"