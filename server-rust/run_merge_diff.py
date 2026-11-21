#!/usr/bin/env python3
"""Merged-tree Go vs Rust differential on a disposable loopback Postgres.

Both backends start with every provider, channel and cloud integration
unconfigured; no outbound provider call is possible. Synthetic JWT lives only
in this process environment for the duration of the compare.
"""
import base64
import re
import hashlib
import hmac
import json
import os
import secrets
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# The oracle is the v2-pinned Go build: /app/server extracted from image
# sha256:3f6835a1461c8... (revision e8ae72c9d, manifest oracle_image_id), which
# is the ZBSI-60 brief pin. It is a different build profile from the earlier
# zbsi63-go-oracle-20261104 (statically linked + stripped, 48 MB vs dynamically
# linked + unstripped, 68 MB), so the hash is asserted rather than assumed: a
# mislabelled oracle would make every differential result meaningless.
ORACLE = "/data/rust-build/zbstech/zbsi60-go-oracle-e8ae72c9d-20261118"
ORACLE_SHA256 = "bafb4054144544df9de09f64bcd4aff4c60a7beced367acbab86adae9e00fe36"
MIGRATE = "/data/rust-build/zbstech/zbsi63-go-migrate-20261104"
MIGRATE_SHA256 = "076f9f9072aa6aacd3a598c2d8434c98ac98b7b352562951c119de4660ad7d4e"
RUST_BIN = "/data/rust-build/zbstech/.multica-rust-sprint-20261102/target-zbsi-100/debug/multica-server"
SECRET = "synthetic_32_character_test_only_secret"
OWNER = f"merge-diff-{int(time.time())}-{os.getpid()}"
STATE = ROOT / "merge-diff-state.json"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run(cmd, **kw):
    return subprocess.run(cmd, check=True, text=True, capture_output=True, **kw)


def wait_healthy(port, proc, tries=120):
    for _ in range(tries):
        if proc.poll() is not None:
            raise RuntimeError(f"server on {port} exited {proc.returncode}")
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1).close()
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"server on {port} never became healthy")


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_binary(path, expected, role):
    if not os.path.exists(path):
        raise RuntimeError(f"{role} not found at {path}")
    actual = file_sha256(path)
    if actual != expected:
        raise RuntimeError(f"{role} hash mismatch at {path}: expected {expected}, got {actual}")
    print(f"{role} verified {actual} {path}", flush=True)


def main():
    verify_binary(ORACLE, ORACLE_SHA256, "go oracle")
    verify_binary(MIGRATE, MIGRATE_SHA256, "go migrate")
    pg_port, go_port, rust_port = free_port(), free_port(), free_port()
    assert len({pg_port, go_port, rust_port}) == 3
    assert 5432 not in (pg_port, go_port, rust_port)
    net = f"merge-diff-net-{os.getpid()}"
    cid = ""
    started = []
    try:
        run(["docker", "network", "create", "--label", f"multica.test.owner={OWNER}", net])
        cid = run(["docker", "run", "-d", "--network", net,
                   "--label", f"multica.test.owner={OWNER}", "--label", "multica.test.issue=merge-A",
                   "--memory=1536m", "--cpus=2",
                   "-e", "POSTGRES_USER=zbsi63", "-e", "POSTGRES_PASSWORD=synthetic_zbsi63",
                   "-e", "POSTGRES_DB=multica_zbsi63go",
                   "-p", f"127.0.0.1:{pg_port}:5432",
                   "-p", f"127.0.0.1:{go_port}:{go_port}",
                   "postgres:17"]).stdout.strip()
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
        print(f"postgres ready on {pg_port}", flush=True)

        go_url = f"postgres://zbsi63:synthetic_zbsi63@127.0.0.1:{pg_port}/multica_zbsi63go?sslmode=disable"
        rust_url = f"postgres://zbsi63:synthetic_zbsi63@127.0.0.1:{pg_port}/multica_zbsi63?sslmode=disable"
        run(["docker", "exec", cid, "psql", "-q", "-U", "zbsi63", "-d", "postgres",
             "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE multica_zbsi63"])

        # Go lineage: embedded migrations of the oracle's own revision (head 594).
        golog = (ROOT / "merge-diff-go-migrate.log").open("w")
        p = subprocess.run([MIGRATE, "up"], env=dict(os.environ, DATABASE_URL=go_url),
                           cwd=str(ROOT / ".test-migrations"), stdout=golog, stderr=subprocess.STDOUT)
        golog.close()
        if p.returncode != 0:
            raise RuntimeError("go migrate failed")
        print("go migrated", flush=True)

        # Rust lineage: upstream stream (through 594 + Rust sprint 595-597) plus zbs stream.
        rlog = (ROOT / "merge-diff-rust-migrate.log").open("w")
        p = subprocess.run([RUST_BIN, "migrate-up"],
                           env=dict(os.environ, RUST_DATABASE_URL=rust_url,
                                    RUST_DEV_DATABASE_CONFIRM="multica_zbsi63",
                                    RUST_MIGRATIONS_ROOT=str(ROOT / ".test-migrations-rust")),
                           stdout=rlog, stderr=subprocess.STDOUT)
        rlog.close()
        if p.returncode != 0:
            raise RuntimeError("rust migrate failed")
        print("rust migrated", flush=True)

        # A real task-token row exercises the auth middleware and the env
        # impersonation guard, not a client-forged actor header. Keep the raw
        # synthetic token in process memory; neither reports nor logs contain it.
        task_token = "mat_zbsi100_" + secrets.token_urlsafe(24)
        task_hash = hashlib.sha256(task_token.encode()).hexdigest()
        seed = """
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
""".replace("__SYNTHETIC_TASK_HASH__", task_hash)
        for db in ("multica_zbsi63go", "multica_zbsi63"):
            p = subprocess.run(["docker", "exec", "-i", cid, "psql", "-q", "-U", "zbsi63", "-d", db,
                                "-v", "ON_ERROR_STOP=1"], input=seed, text=True, capture_output=True)
            if p.returncode != 0:
                raise RuntimeError(f"seed failed for {db}: {p.stderr[:400]}")
        print("seeded", flush=True)

        go_cid = run(["docker", "run", "-d", "--network", f"container:{cid}",
                      "--label", f"multica.test.owner={OWNER}", "--label", "multica.test.issue=merge-A",
                      "--memory=1024m", "--cpus=2",
                      "-e", f"DATABASE_URL=postgres://zbsi63:synthetic_zbsi63@127.0.0.1:5432/multica_zbsi63go?sslmode=disable",
                      "-e", f"PORT={go_port}", "-e", f"JWT_SECRET={SECRET}", "-e", "DO_NOT_TRACK=1",
                      "-v", f"{ORACLE}:/oracle:ro", "postgres:17", "/oracle"]).stdout.strip()

        rust_log = (ROOT / "merge-diff-rust-server.log").open("w")
        rust_proc = subprocess.Popen(
            [RUST_BIN],
            env={"PATH": os.environ["PATH"], "RUST_DATABASE_URL": rust_url,
                 "RUST_BIND_ADDR": f"127.0.0.1:{rust_port}", "JWT_SECRET": SECRET},
            stdout=rust_log, stderr=subprocess.STDOUT, start_new_session=True)
        started.append(rust_proc)

        class Ghost:
            def poll(self):
                return None
        wait_healthy(go_port, Ghost())
        wait_healthy(rust_port, rust_proc)
        print(f"both healthy (go {go_port}, rust {rust_port})", flush=True)

        enc = lambda o: base64.urlsafe_b64encode(
            json.dumps(o, separators=(",", ":")).encode()).decode().rstrip("=")
        now = int(time.time())
        def jwt(subject, email, name):
            unsigned = enc({"alg": "HS256", "typ": "JWT"}) + "." + enc({
                "sub": subject, "email": email, "name": name,
                "sid": "synthetic-session", "iat": now, "exp": now + 3600})
            sig = base64.urlsafe_b64encode(
                hmac.new(SECRET.encode(), unsigned.encode(), hashlib.sha256).digest()).decode().rstrip("=")
            return unsigned + "." + sig

        token = jwt("00000000-0000-0000-0000-0000000000a2",
                    "synthetic-zbsi63@example.invalid", "Synthetic User")
        member_token = jwt("00000000-0000-0000-0000-0000000000a8",
                           "synthetic-other-member@example.invalid", "Other Member")
        env = dict(os.environ, LAB_BEARER=token, RUST_LAB_BEARER=token,
                   LAB_MEMBER_BEARER=member_token, RUST_LAB_MEMBER_BEARER=member_token,
                   LAB_TASK_BEARER=task_token, RUST_LAB_TASK_BEARER=task_token)

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None

        opener = urllib.request.build_opener(NoRedirect)

        def probe(port, path, method="GET", body=None):
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}{path}", method=method,
                data=json.dumps(body).encode() if body is not None else None,
                headers={"Authorization": "Bearer " + token, "Accept": "application/json",
                         "Content-Type": "application/json",
                         "X-Workspace-ID": "00000000-0000-0000-0000-0000000000a1"})
            try:
                with opener.open(req, timeout=10) as r:
                    return r.status, r.headers.get("Location"), r.headers.get("Content-Type", "")
            except urllib.error.HTTPError as e:
                return e.code, e.headers.get("Location"), e.headers.get("Content-Type", "")

        probes = [
            "/api/projects/00000000-0000-0000-0000-0000000000a6",
            "/api/issues/00000000-0000-0000-0000-0000000000a7",
            "/api/labels", "/api/squads", "/api/skills", "/api/runtimes", "/api/inbox",
            "/api/pins", "/api/properties", "/api/issue-views", "/api/issue-statuses",
            "/api/quick-actions", "/api/autopilots", "/api/notification-preferences",
            # Registered without a trailing slash in Rust: check whether Go also
            # answers the trailing-slash form (i.e. whether chi is symmetric).
            "/api/chat/sessions", "/api/agents", "/api/workspaces", "/api/cloud-billing/balance",
            # Static GET routes, to size the trailing-slash gap across the tree.
            "/api/agent-activity-30d", "/api/me", "/api/inbox/unread-count",
            "/api/chat/pending-tasks/has-any", "/api/issues/search", "/api/projects/search",
            "/api/issue-workflows", "/api/tokens", "/api/limits", "/api/cloud-subscriptions/prices",
        ]
        probe_rows = {}
        for path in probes:
            if path == "/api/issues/search":
                variants = (path + "?q=synthetic",)
            else:
                variants = (path, path + "/")
            for variant in variants:
                row = {"go": probe(go_port, variant), "rust": probe(rust_port, variant)}
                probe_rows[variant] = row
                print(f"probe {variant} go={row['go']} rust={row['rust']}", flush=True)
        (ROOT / "merge-diff-slash-probe.json").write_text(json.dumps(probe_rows, indent=2))

        fixture = str(ROOT / "tools/zbsi63-lab-v2/synthetic-fixture.json")
        lab = str(ROOT / "tools/zbsi63-lab-v2")
        reports = {}
        for label, cases, extra, fixtures in (
            ("base-all", f"{lab}/cases.json", [], fixture),
            ("read-supplemental", f"{lab}/read-supplemental-cases.json", [], fixture),
            ("workspace-isolation", f"{lab}/workspace-isolation-cases.json", [],
             f"{lab}/synthetic-other-workspace.json"),
            ("p0", str(ROOT / "p0-cases-v2.json"), [], fixture),
            ("p1", f"{lab}/p1-cases.json", ["--group", "p1"], fixture),
        ):
            out = ROOT / f"merge-diff-{label}-report.json"
            if out.exists():
                out.unlink()
            cmd = [sys.executable, str(ROOT / "tools/zbsi63-lab-v2/lab.py"), "compare",
                   "--base", f"http://127.0.0.1:{go_port}",
                   "--candidate", f"http://127.0.0.1:{rust_port}",
                   "--fixtures", fixtures, "--candidate-fixtures", fixtures,
                   "--cases", cases, "--out", str(out),
                   "--allow-writes", "--isolated-target"] + extra
            p = subprocess.run(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True)
            reports[label] = {"exit": p.returncode,
                              "stdout": p.stdout.strip(), "stderr": p.stderr.strip(),
                              "report": str(out.name)}
            print(f"{label}: exit {p.returncode} {p.stdout.strip()} {p.stderr.strip()}", flush=True)

        # Full route probe, both slash variants, after the case groups so any
        # write-side state churn cannot perturb the comparisons above.
        param = "00000000-0000-0000-0000-0000000000f9"
        rows = []
        for line in (ROOT / "go_routes_merge.txt").read_text().splitlines():
            parts = line.split()
            if len(parts) != 2:
                continue
            method, template = parts
            path = re.sub(r"\{[^}]+\}", param, template)
            for variant in (path, path + "/"):
                go_s, go_ct = probe(go_port, variant, method)[0::2]
                rs_s, rs_ct = probe(rust_port, variant, method)[0::2]
                rows.append({"method": method, "template": template,
                             "path": variant, "slash": variant.endswith("/"),
                             "go": [go_s, go_ct.split(";")[0]], "rust": [rs_s, rs_ct.split(";")[0]]})
        (ROOT / "merge-diff-route-probe.json").write_text(json.dumps(rows, indent=1))
        print(f"route probe rows: {len(rows)}", flush=True)

        summary = {"owner": OWNER, "ports": {"pg": pg_port, "go": go_port, "rust": rust_port},
                   "oracle": {"path": ORACLE, "sha256": ORACLE_SHA256},
                   "migrate": {"path": MIGRATE, "sha256": MIGRATE_SHA256},
                   "rust_bin": {"path": RUST_BIN, "sha256": file_sha256(RUST_BIN)},
                   "reports": reports}
        (ROOT / "merge-diff-summary.json").write_text(json.dumps(summary, indent=2))
        return 0 if all(r["exit"] == 0 for r in reports.values()) else 1
    finally:
        for proc in started:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        for name, c in (("go", locals().get("go_cid", "")), ("pg", cid)):
            if c:
                label = subprocess.run(
                    ["docker", "inspect", "-f", '{{index .Config.Labels "multica.test.owner"}}', c],
                    capture_output=True, text=True).stdout.strip()
                if label == OWNER:
                    subprocess.run(["docker", "rm", "-f", c], capture_output=True)
        label = subprocess.run(
            ["docker", "network", "inspect", "-f", '{{index .Labels "multica.test.owner"}}', net],
            capture_output=True, text=True).stdout.strip()
        if label == OWNER:
            subprocess.run(["docker", "network", "rm", net], capture_output=True)


if __name__ == "__main__":
    sys.exit(main())
