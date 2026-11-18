#!/usr/bin/env bash
# Run only through sprint-gate.py --kind db. No shared target or database access.
#
# Query-domain DB gate for ZBSI-69 against the integrated server-rust tree.
# Each batch opens its own owned disposable PostgreSQL 16 and migration run so
# a cold build or a long suite cannot consume another batch's 480s gate window.
#
# The v52 checkpoint split the suites into six batches. Two v52 test targets no
# longer exist in the integrated tree:
#   - queries_sprint_table_sorts -> sort matrices inside
#     tests/queries_sprint_table_rows.rs and tests/queries_sprint_issue_list.rs.
# No suite is silently skipped: every target present in the tree is in a batch.
set -euo pipefail
cd "$(dirname "$0")"
[[ "${CARGO_TARGET_DIR:-}" == /data/rust-build/zbstech/.multica-rust-sprint-20261102/target-zbsi-69 ]] || { printf 'Run through ZBSI-69 sprint gate\n' >&2; exit 2; }
owner="queries-sprint-zbsi69-$(date +%s)-$$"
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
  -e POSTGRES_USER=queries_sprint -e POSTGRES_PASSWORD=synthetic_queries_sprint \
  -e POSTGRES_DB=multica_zbsi69 -p 127.0.0.1::5432 postgres:16)
port=$(docker port "$cid" 5432/tcp | cut -d: -f2)
[[ "$port" =~ ^[0-9]+$ && "$port" != 5432 ]] || { printf 'Refusing invalid or shared port\n' >&2; exit 1; }
ready=false
for i in $(seq 1 60); do
  if docker exec "$cid" pg_isready -h 127.0.0.1 -U queries_sprint -d multica_zbsi69 >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { printf 'PostgreSQL readiness timeout\n' >&2; exit 1; }
export RUSTUP_TOOLCHAIN=1.95.0
cargo=/home/zbstech/.cargo/bin/cargo
url="postgres://queries_sprint:synthetic_queries_sprint@127.0.0.1:$port/multica_zbsi69"
printf 'Owned disposable PostgreSQL 16 ready on loopback:%s\n' "$port"
RUST_DATABASE_URL="$url" RUST_DEV_DATABASE_CONFIRM=multica_zbsi69 RUST_MIGRATIONS_ROOT="$PWD" \
  "$cargo" run --locked --offline -q -- migrate-up
result=0
if [[ "${QUERIES_SPRINT_ONLY_PULL_REQUESTS:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --lib queries_sprint_pull_requests -- --nocapture || result=$?
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_pull_requests -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_TIMELINE:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_timeline -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_COMMENTS:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_comments -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_TASK_SNAPSHOT:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_task_snapshot -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_LIMIT_USAGE:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_limit_usage -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_DETAIL:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_issue_detail -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_GROUPS:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_table_groups -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_FACETS:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_table_facets -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_TABLE:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_table_rows -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ "${QUERIES_SPRINT_ONLY_LIST:-}" == 1 ]]; then
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test queries_sprint_issue_list -- --ignored --nocapture --test-threads=1 || result=$?
  exit "$result"
fi
if [[ -n "${QUERIES_SPRINT_BATCH:-}" ]]; then
  case "$QUERIES_SPRINT_BATCH" in
    1) suites=(queries_sprint_pull_requests queries_sprint_timeline) ;;
    2) suites=(queries_sprint_issue_detail queries_sprint_limit_usage queries_sprint_inbox queries_sprint_search) ;;
    3) suites=(queries_sprint_pins queries_sprint_quick_action_writes queries_sprint_issue_search queries_sprint_task_snapshot) ;;
    4) suites=(queries_sprint_issue_list queries_sprint_table_rows queries_sprint_table_facets queries_sprint_table_groups) ;;
    5) suites=(queries_sprint_comments) ;;
    6)
      RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --lib queries_sprint_ -- --include-ignored --nocapture --test-threads=1 || result=$?
      exit "$result"
      ;;
    *) printf 'Invalid query suite batch\n' >&2; exit 2 ;;
  esac
  for suite in "${suites[@]}"; do
    RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test "$suite" -- --ignored --nocapture --test-threads=1 || result=$?
  done
  exit "$result"
fi
RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --lib queries_sprint_ -- --include-ignored --nocapture --test-threads=1 || result=$?
for suite in queries_sprint_pull_requests queries_sprint_timeline queries_sprint_comments queries_sprint_issue_detail queries_sprint_limit_usage queries_sprint_inbox queries_sprint_search queries_sprint_pins queries_sprint_quick_action_writes queries_sprint_issue_search queries_sprint_task_snapshot queries_sprint_issue_list queries_sprint_table_rows queries_sprint_table_facets queries_sprint_table_groups; do
  RUST_API_TEST_DATABASE_URL="$url" "$cargo" test --locked --offline --test "$suite" -- --ignored --nocapture --test-threads=1 || result=$?
done
exit "$result"
