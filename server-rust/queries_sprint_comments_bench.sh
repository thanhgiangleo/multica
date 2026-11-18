#!/usr/bin/env bash
# Run only through sprint-gate.py --kind db. No shared target or database access.
#
# Cost probe for the frozen recent-window projection (queries_sprint_comments_recent.sql)
# at the 20k-comment shape used by tests/queries_sprint_comments.rs: one root with
# 20,001 replies plus an older thread, and a malformed cross-tenant child. The query
# text is the file the handler embeds, prepared with the handler's parameter types, so
# the plan and timing are the server's, not a rephrased copy.
set -euo pipefail
cd "$(dirname "$0")"
[[ "${CARGO_TARGET_DIR:-}" == /data/rust-build/zbstech/.multica-rust-sprint-20261102/target-zbsi-69 ]] || { printf 'Run through ZBSI-69 sprint gate\n' >&2; exit 2; }
owner="queries-bench-zbsi69-$(date +%s)-$$"
cid=""
cleanup() {
  rm -f .bench-seed.sql .bench-explain.sql .bench-explain.out
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
psql_db() { docker exec -i "$cid" psql -U queries_sprint -d multica_zbsi69 -v ON_ERROR_STOP=1 -q "$@"; }

user=$(psql_db -tAc "INSERT INTO \"user\"(name,email) VALUES ('bench','bench@invalid.example') RETURNING id")
ws=$(psql_db -tAc "INSERT INTO workspace(name,slug) VALUES ('Bench','bench-$RANDOM') RETURNING id")
other=$(psql_db -tAc "INSERT INTO workspace(name,slug) VALUES ('Other','other-$RANDOM') RETURNING id")
psql_db -c "INSERT INTO member(workspace_id,user_id,role) VALUES ('$ws','$user','owner'),('$other','$user','owner')"
issue=$(psql_db -tAc "INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ('$ws','bench','member','$user','todo','medium',1) RETURNING id")
foreign_issue=$(psql_db -tAc "INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ('$other','bench','member','$user','todo','medium',1) RETURNING id")
old=$(psql_db -tAc "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,created_at) VALUES ('$ws','$issue','member','$user','older thread',now()-interval '100 days') RETURNING id")
root=$(psql_db -tAc "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,created_at) VALUES ('$ws','$issue','member','$user','root',now()) RETURNING id")
psql_db -c "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) SELECT '$ws','$issue','member','$user','reply','$root',now()+(n/3)*interval '1 microsecond' FROM generate_series(1,20001) n"
psql_db -c "INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,parent_id,created_at) VALUES ('$other','$foreign_issue','member','$user','PRIVATE','$root',now())"
printf 'Seeded: comments=%s roots=%s\n' \
  "$(psql_db -tAc "SELECT count(*) FROM comment WHERE workspace_id='$ws' AND issue_id='$issue'")" \
  "$(psql_db -tAc "SELECT count(*) FROM comment WHERE workspace_id='$ws' AND parent_id IS NULL")"

{
  printf 'PREPARE q(uuid,uuid,boolean,timestamptz,uuid,int4) AS\n'
  cat src/api/queries_sprint_comments_recent.sql
  printf ';\n\\timing on\n'
  # Phase 1 keeps the statistics PostgreSQL has right after a bulk insert, which
  # is what the HTTP test sees; phase 2 repeats on freshly analyzed statistics.
  for phase in inserted analyzed; do
    for limit in 1 50; do
      for run in cold warm; do
        printf '\\echo === stats=%s limit=%s run=%s\n' "$phase" "$limit" "$run"
        printf "EXPLAIN (ANALYZE, BUFFERS) EXECUTE q('%s','%s',false,'epoch'::timestamptz,'00000000-0000-0000-0000-000000000000'::uuid,%s);\n" \
          "$issue" "$ws" "$limit"
      done
    done
    if [[ "$phase" == inserted ]]; then printf 'ANALYZE comment;\n'; fi
  done
} > .bench-explain.sql
psql_db -f /dev/stdin < .bench-explain.sql | tee .bench-explain.out
# A zero-row plan costs nothing; refuse to report a measurement that read no thread.
grep -q 'rows=20002' .bench-explain.out || { printf 'Refusing: plan returned no thread rows\n' >&2; exit 1; }
grep -E '^ Execution Time' .bench-explain.out
printf 'Bench done on disposable PostgreSQL 16\n'