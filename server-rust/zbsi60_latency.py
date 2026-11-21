#!/usr/bin/env python3
"""Per-case GET latency on the same disposable loopback stack as the differential.

Every GET case in the five compared groups is sampled on both backends with the
same synthetic fixtures, n=100 successful samples, closed loop at concurrency 1,
warmup 5. A case that cannot reach 100 successes is reported with an explicit
`n_below_100` flag instead of being dropped or averaged away.

The stack (network, PostgreSQL 17, Go migrations, Rust migrations, seed) mirrors
run_merge_diff.py: the two must describe the same database, so the seed is kept
byte-identical to that harness. If the harness fixtures change, change these too.
"""
import argparse
import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

import run_merge_diff as merge

ROOT = Path(__file__).resolve().parent
LAB = ROOT / "tools/zbsi63-lab-v2"
sys.path.insert(0, str(LAB))
import lab  # noqa: E402  (same assertions the contract compare uses)
RUST_BIN = "/data/rust-build/zbstech/.multica-rust-sprint-20261102/target-zbsi-60/debug/multica-server"

GROUPS = (
    ("base-all", LAB / "cases.json", [], LAB / "synthetic-fixture.json"),
    ("read-supplemental", LAB / "read-supplemental-cases.json", [],
     LAB / "synthetic-fixture.json"),
    ("workspace-isolation", LAB / "workspace-isolation-cases.json", [],
     LAB / "synthetic-other-workspace.json"),
    ("p0", ROOT / "p0-cases-v2.json", [], LAB / "synthetic-fixture.json"),
    ("p1", LAB / "p1-cases.json", ["--group", "p1"], LAB / "synthetic-fixture.json"),
)

SEED = """
INSERT INTO "user" (id,name,email) VALUES ('00000000-0000-0000-0000-0000000000a2','Synthetic User','synthetic-zbsi63@example.invalid');
INSERT INTO workspace (id,name,slug) VALUES ('00000000-0000-0000-0000-0000000000a1','Synthetic Workspace','synthetic-zbsi63'),('00000000-0000-0000-0000-0000000000b1','Other Workspace','other-zbsi63');
INSERT INTO member (workspace_id,user_id,role) VALUES ('00000000-0000-0000-0000-0000000000a1','00000000-0000-0000-0000-0000000000a2','owner');
-- No daemon connects: this runtime exercises enqueue/persistence only, never execution.
INSERT INTO agent_runtime (id,workspace_id,name,runtime_mode,provider,owner_id,status) VALUES ('00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a1','Synthetic Runtime','local','synthetic','00000000-0000-0000-0000-0000000000a2','offline');
INSERT INTO agent (id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ('00000000-0000-0000-0000-0000000000a3','00000000-0000-0000-0000-0000000000a1','Synthetic Agent','local','00000000-0000-0000-0000-0000000000a2','00000000-0000-0000-0000-0000000000a9');
INSERT INTO chat_session (id,workspace_id,agent_id,creator_id,title,status,explicitly_created_at) VALUES ('00000000-0000-0000-0000-0000000000a4','00000000-0000-0000-0000-0000000000a1','00000000-0000-0000-0000-0000000000a3','00000000-0000-0000-0000-0000000000a2','Synthetic session','active',now());
INSERT INTO project (id,workspace_id,title) VALUES ('00000000-0000-0000-0000-0000000000a6','00000000-0000-0000-0000-0000000000a1','Synthetic Project');
INSERT INTO issue (id,workspace_id,title,creator_type,creator_id) VALUES ('00000000-0000-0000-0000-0000000000a7','00000000-0000-0000-0000-0000000000a1','Synthetic Issue','member','00000000-0000-0000-0000-0000000000a2');
INSERT INTO "user" (id,name,email) VALUES ('00000000-0000-0000-0000-0000000000a8','Other Member','synthetic-other-member@example.invalid');
INSERT INTO member (workspace_id,user_id,role) VALUES ('00000000-0000-0000-0000-0000000000a1','00000000-0000-0000-0000-0000000000a8','member');
INSERT INTO chat_session (id,workspace_id,agent_id,creator_id,title,status,explicitly_created_at) VALUES ('00000000-0000-0000-0000-0000000000a5','00000000-0000-0000-0000-0000000000a1','00000000-0000-0000-0000-0000000000a3','00000000-0000-0000-0000-0000000000a8','Other member session','active',now());
-- Isolated P0 mutation fixtures: the existing five case groups still use the original agent.
INSERT INTO agent (id,workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode,custom_env) VALUES
 ('00000000-0000-0000-0000-0000000000c1','00000000-0000-0000-0000-0000000000a1','Archive Fixture','local','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a2','private','{"KEEP":"synthetic-value"}'),
 ('00000000-0000-0000-0000-0000000000c2','00000000-0000-0000-0000-0000000000a1','Cancel Fixture','local','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a2','private','{}'),
 ('00000000-0000-0000-0000-0000000000c3','00000000-0000-0000-0000-0000000000a1','Env Fixture','local','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a2','private','{"KEEP":"synthetic-value"}'),
 ('00000000-0000-0000-0000-0000000000c5','00000000-0000-0000-0000-0000000000a1','Built-in Fixture','local','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a2','private','{}'),
 ('00000000-0000-0000-0000-0000000000c6','00000000-0000-0000-0000-0000000000a1','Member-owned Fixture','local','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a8','private','{"KEEP":"member-owned"}');
UPDATE agent SET system_key='synthetic-builtin' WHERE id='00000000-0000-0000-0000-0000000000c5';
INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,permission_mode) VALUES
 ('00000000-0000-0000-0000-0000000000c7','00000000-0000-0000-0000-0000000000b1','Foreign Fixture','local','00000000-0000-0000-0000-0000000000a8','private');
-- Renamed built-in agent: the product prompt must follow the current name,
-- exactly as the Go binary's embedded instructions do.
INSERT INTO agent (id,workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode,custom_env) VALUES
 ('00000000-0000-0000-0000-0000000000c8','00000000-0000-0000-0000-0000000000a1','Synthetic Chief','local','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a2','private','{}');
UPDATE agent SET system_key='mika' WHERE id='00000000-0000-0000-0000-0000000000c8';
INSERT INTO skill(id,workspace_id,name,description) VALUES ('00000000-0000-0000-0000-0000000000c4','00000000-0000-0000-0000-0000000000a1','Archive Skill','Synthetic summary');
INSERT INTO agent_skill(agent_id,skill_id) VALUES ('00000000-0000-0000-0000-0000000000c1','00000000-0000-0000-0000-0000000000c4');
INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES
 ('00000000-0000-0000-0000-0000000000d1','00000000-0000-0000-0000-0000000000c2','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a7','running'),
 ('00000000-0000-0000-0000-0000000000d2','00000000-0000-0000-0000-0000000000c2','00000000-0000-0000-0000-0000000000a9',NULL,'queued'),
 ('00000000-0000-0000-0000-0000000000d3','00000000-0000-0000-0000-0000000000a3','00000000-0000-0000-0000-0000000000a9','00000000-0000-0000-0000-0000000000a7','queued'),
 ('00000000-0000-0000-0000-0000000000d4','00000000-0000-0000-0000-0000000000a3','00000000-0000-0000-0000-0000000000a9',NULL,'queued');
INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES
 ('__SYNTHETIC_TASK_HASH__','00000000-0000-0000-0000-0000000000d1','00000000-0000-0000-0000-0000000000c2','00000000-0000-0000-0000-0000000000a1','00000000-0000-0000-0000-0000000000a2',now()+interval '1 hour');
"""


# These four GET cases assert values captured from the chat write sequence, so
# they only hold after that sequence ran against the same backend. Sampling them
# standalone would measure a 404, not the endpoint.
CHAT_FLOW_IDS = {"empty-page", "first-turn-page", "queued-turn-not-in-page",
                 "pending-includes-queued-turn"}


def get_cases():
    """Every GET case, keeping each id once: the P0 file repeats four base cases."""
    seen, rows = set(), []
    for label, cases, extra, fixtures in GROUPS:
        wanted = extra[extra.index("--group") + 1:] if "--group" in extra else []
        for case in json.loads(cases.read_text())["cases"]:
            if wanted and case.get("group") not in wanted:
                continue
            if case.get("method", "GET") != "GET" or case["id"] in seen:
                continue
            seen.add(case["id"])
            if case["id"] in CHAT_FLOW_IDS:
                continue
            rows.append({"group": label, "id": case["id"], "path": case["path"],
                         "cases": cases, "extra": extra, "fixtures": fixtures})
    return rows


def chat_flow_bench(backend, base, prefix, args):
    """Replay the chat write sequence, then sample the four page cases it enables."""
    cases = {c["id"]: c for c in
             json.loads((LAB / "cases.json").read_text())["cases"]}
    variables = dict(json.loads((LAB / "synthetic-fixture.json").read_text()))
    rows = []

    def write(case_id):
        case = cases[case_id]
        status, document, _ = lab.request(base, case, variables, prefix)
        lab.check_response(case, status, document, variables)

    def bench(case_id):
        case = cases[case_id]
        values, failures, warmup_failures = [], 0, 0
        for i in range(args.samples + args.warmup):
            start = time.perf_counter()
            try:
                status, document, elapsed = lab.request(base, case, variables, prefix)
                lab.check_response(case, status, document, dict(variables))
                if i >= args.warmup:
                    values.append(elapsed)
            except lab.LabError:
                if i < args.warmup:
                    warmup_failures += 1
                else:
                    failures += 1
            time.sleep(max(0, 1 / args.rps - (time.perf_counter() - start)))
        latency = lab.summarize(values)
        rows.append({"backend": backend, "group": "chat-flow", "case": case_id,
                     "path": case["path"], "exit": 0 if failures == 0 and
                     warmup_failures == 0 else 1,
                     "n": latency["n"], "p50_ms": latency["p50_ms"],
                     "p95_ms": latency["p95_ms"], "p99_ms": latency["p99_ms"],
                     "failures": failures, "warmup_failures": warmup_failures,
                     "n_below_100": latency["n"] < 100})
        print(f"{backend} {case_id} n={latency['n']} p95={latency['p95_ms']} "
              f"fail={failures}", flush=True)

    # Same order as the case file: each GET observes the state the preceding
    # writes produced, so the assertions are the compare suite's assertions.
    write("create-session")
    bench("empty-page")
    write("rename-created-session")
    write("send-first-turn")
    bench("first-turn-page")
    write("send-queued-turn")
    bench("queued-turn-not-in-page")
    bench("pending-includes-queued-turn")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-bin", default=RUST_BIN)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--rps", type=float, default=20.0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if Path(args.out).exists():
        raise SystemExit("output already exists; choose a new report path")

    merge.verify_binary(merge.ORACLE, merge.ORACLE_SHA256, "go oracle")
    merge.verify_binary(merge.MIGRATE, merge.MIGRATE_SHA256, "go migrate")
    cases = get_cases()
    print(f"{len(cases)} GET cases", flush=True)

    pg_port, go_port, rust_port = merge.free_port(), merge.free_port(), merge.free_port()
    assert len({pg_port, go_port, rust_port}) == 3 and 5432 not in (pg_port, go_port, rust_port)
    net = f"zbsi60-latency-net-{os.getpid()}"
    owner = f"zbsi60-latency-{int(time.time())}-{os.getpid()}"
    cid = go_cid = ""
    started = []
    reports = []
    try:
        merge.run(["docker", "network", "create", "--label", f"multica.test.owner={owner}", net])
        cid = merge.run(["docker", "run", "-d", "--network", net,
                         "--label", f"multica.test.owner={owner}", "--memory=1536m", "--cpus=2",
                         "-e", "POSTGRES_USER=zbsi63", "-e", "POSTGRES_PASSWORD=synthetic_zbsi63",
                         "-e", "POSTGRES_DB=multica_zbsi63go",
                         "-p", f"127.0.0.1:{pg_port}:5432",
                         "-p", f"127.0.0.1:{go_port}:{go_port}", "postgres:17"]).stdout.strip()
        ready = 0
        for _ in range(90):
            if subprocess.run(["docker", "exec", cid, "psql", "-q", "-U", "zbsi63", "-d",
                               "multica_zbsi63go", "-tAc", "select 1"],
                              capture_output=True).returncode == 0:
                ready += 1
                if ready >= 3:
                    break
            else:
                ready = 0
            time.sleep(1)
        if ready < 3:
            raise RuntimeError("postgres never became ready")
        go_url = (f"postgres://zbsi63:synthetic_zbsi63@127.0.0.1:{pg_port}/"
                  "multica_zbsi63go?sslmode=disable")
        rust_url = (f"postgres://zbsi63:synthetic_zbsi63@127.0.0.1:{pg_port}/"
                    "multica_zbsi63?sslmode=disable")
        merge.run(["docker", "exec", cid, "psql", "-q", "-U", "zbsi63", "-d", "postgres",
                   "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE multica_zbsi63"])
        p = subprocess.run([merge.MIGRATE, "up"], env=dict(os.environ, DATABASE_URL=go_url),
                           cwd=str(ROOT / ".test-migrations"), capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError("go migrate failed: " + p.stderr[-400:])
        p = subprocess.run([args.rust_bin, "migrate-up"],
                           env=dict(os.environ, RUST_DATABASE_URL=rust_url,
                                    RUST_DEV_DATABASE_CONFIRM="multica_zbsi63",
                                    RUST_MIGRATIONS_ROOT=str(ROOT / ".test-migrations-rust")),
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError("rust migrate failed: " + p.stderr[-400:])

        task_token = "mat_zbsi60_" + secrets.token_urlsafe(24)
        task_hash = hashlib.sha256(task_token.encode()).hexdigest()
        seed = SEED.replace("__SYNTHETIC_TASK_HASH__", task_hash)
        for db in ("multica_zbsi63go", "multica_zbsi63"):
            p = subprocess.run(["docker", "exec", "-i", cid, "psql", "-q", "-U", "zbsi63",
                                "-d", db, "-v", "ON_ERROR_STOP=1"],
                               input=seed, text=True, capture_output=True)
            if p.returncode != 0:
                raise RuntimeError(f"seed failed for {db}: {p.stderr[:400]}")

        # Inside the shared namespace PostgreSQL is on its own 5432, not the
        # host-published port the migrations used.
        go_container_url = ("postgres://zbsi63:synthetic_zbsi63@127.0.0.1:5432/"
                            "multica_zbsi63go?sslmode=disable")
        go_cid = merge.run(["docker", "run", "-d", "--network", f"container:{cid}",
                            "--label", f"multica.test.owner={owner}", "--memory=1024m", "--cpus=2",
                            "-e", f"DATABASE_URL={go_container_url}", "-e", f"PORT={go_port}",
                            "-e", f"JWT_SECRET={merge.SECRET}", "-e", "DO_NOT_TRACK=1",
                            "-v", f"{merge.ORACLE}:/oracle:ro", "postgres:17", "/oracle"]
                           ).stdout.strip()
        rust_log = (ROOT / "zbsi60-latency-rust-server.log").open("w")
        rust_proc = subprocess.Popen(
            [args.rust_bin],
            env={"PATH": os.environ["PATH"], "RUST_DATABASE_URL": rust_url,
                 "RUST_BIND_ADDR": f"127.0.0.1:{rust_port}", "JWT_SECRET": merge.SECRET},
            stdout=rust_log, stderr=subprocess.STDOUT, start_new_session=True)
        started.append(rust_proc)

        class Ghost:
            def poll(self):
                return None

        merge.wait_healthy(go_port, Ghost())
        merge.wait_healthy(rust_port, rust_proc)
        print(f"both healthy (go {go_port}, rust {rust_port}); rust {args.rust_bin}",
              flush=True)

        import base64
        import hmac

        def enc(obj):
            return base64.urlsafe_b64encode(
                json.dumps(obj, separators=(",", ":")).encode()).decode().rstrip("=")

        now = int(time.time())

        def jwt(subject, email, name):
            unsigned = enc({"alg": "HS256", "typ": "JWT"}) + "." + enc({
                "sub": subject, "email": email, "name": name,
                "sid": "synthetic-session", "iat": now, "exp": now + 3600})
            sig = base64.urlsafe_b64encode(
                hmac.new(merge.SECRET.encode(), unsigned.encode(),
                         hashlib.sha256).digest()).decode().rstrip("=")
            return unsigned + "." + sig

        env = dict(os.environ,
                   LAB_BEARER=jwt("00000000-0000-0000-0000-0000000000a2",
                                  "synthetic-zbsi63@example.invalid", "Synthetic User"),
                   RUST_LAB_BEARER=jwt("00000000-0000-0000-0000-0000000000a2",
                                       "synthetic-zbsi63@example.invalid", "Synthetic User"),
                   LAB_MEMBER_BEARER=jwt("00000000-0000-0000-0000-0000000000a8",
                                         "synthetic-other-member@example.invalid", "Other Member"),
                   RUST_LAB_MEMBER_BEARER=jwt("00000000-0000-0000-0000-0000000000a8",
                                              "synthetic-other-member@example.invalid",
                                              "Other Member"),
                   LAB_TASK_BEARER=task_token, RUST_LAB_TASK_BEARER=task_token)
        # lab.request reads credentials from the process environment; the
        # subprocesses get the same values through `env`.
        os.environ.update(env)

        out_dir = ROOT / ".zbsi60-latency"
        out_dir.mkdir(exist_ok=True)
        for backend, port, prefix in (("go", go_port, "LAB_"),
                                      ("rust", rust_port, "RUST_LAB_")):
            for case in cases:
                target = out_dir / f"{backend}-{case['id']}.json"
                if target.exists():
                    target.unlink()
                cmd = [sys.executable, str(LAB / "lab.py"), "http",
                       "--base", f"http://127.0.0.1:{port}",
                       "--fixtures", str(case["fixtures"]),
                       "--cases", str(case["cases"]),
                       "--case", case["id"], "--prefix", prefix,
                       "--samples", str(args.samples), "--warmup", str(args.warmup),
                       "--rps", str(args.rps), "--out", str(target)] + case["extra"]
                p = subprocess.run(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True)
                row = {"backend": backend, "group": case["group"], "case": case["id"],
                       "path": case["path"], "exit": p.returncode}
                if p.returncode == 0:
                    body = json.loads(target.read_text())
                    latency = body["successful_latency"]
                    row.update(n=latency["n"], p50_ms=latency.get("p50_ms"),
                               p95_ms=latency.get("p95_ms"), p99_ms=latency.get("p99_ms"),
                               failures=body["failures"],
                               warmup_failures=body["warmup_failures"],
                               n_below_100=latency["n"] < 100)
                else:
                    row["error"] = (p.stderr or p.stdout).strip()[-300:]
                    row["n_below_100"] = True
                reports.append(row)
                print(f"{backend} {case['id']} exit={p.returncode} n={row.get('n')} "
                      f"p95={row.get('p95_ms')} fail={row.get('failures')}", flush=True)
            reports.extend(chat_flow_bench(backend, f"http://127.0.0.1:{port}", prefix, args))
    finally:
        for proc in started:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        for name, container in (("go", go_cid), ("pg", cid)):
            if container:
                label = subprocess.run(
                    ["docker", "inspect", "-f",
                     '{{index .Config.Labels "multica.test.owner"}}', container],
                    capture_output=True, text=True).stdout.strip()
                if label == owner:
                    subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        label = subprocess.run(
            ["docker", "network", "inspect", "-f", '{{index .Labels "multica.test.owner"}}', net],
            capture_output=True, text=True).stdout.strip()
        if label == owner:
            subprocess.run(["docker", "network", "rm", net], capture_output=True)

    measured = [r for r in reports if r["exit"] == 0]
    below = [f"{r['backend']}:{r['case']}" for r in reports
             if r.get("n_below_100") or r["exit"] != 0]
    Path(args.out).write_text(json.dumps({
        "samples": args.samples, "warmup": args.warmup, "rps_cap": args.rps,
        "concurrency": 1,
        "rust_bin": {"path": args.rust_bin, "sha256": merge.file_sha256(args.rust_bin)},
        "oracle": {"path": merge.ORACLE, "sha256": merge.ORACLE_SHA256},
        "warning": ("Includes network/client overhead; success-only latency; closed loop, "
                    "not saturation testing; coordinated omission applies."),
        "cases": reports, "below_100": below,
    }, indent=1))
    print(f"measured {len(measured)}/{len(reports)}; below 100: {below}", flush=True)
    return 0 if measured and not below else 1


if __name__ == "__main__":
    sys.exit(main())