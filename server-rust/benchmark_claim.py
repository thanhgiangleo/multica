#!/usr/bin/env python3
"""Reproducible, serial Go/Rust daemon-claim comparison on disposable PostgreSQL 17.

Run from any directory: python3 server-rust/benchmark_claim.py [--samples 100].
Requires Docker, the locally cached Go 1.26.9 toolchain and Cargo. No application
credentials or integrations are inherited by either server. All IDs are synthetic.
"""
import argparse
import contextlib
import shutil
import base64
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
GO = REPO / "server"
REPORT = HERE / "benchmark_claim_report.json"
USER = "00000000-0000-0000-0000-0000000000a2"
WORKSPACE = "00000000-0000-0000-0000-0000000000a1"
RUNTIME = "00000000-0000-0000-0000-0000000000a9"
AGENT = "00000000-0000-0000-0000-0000000000a3"
ISSUE = "00000000-0000-0000-0000-0000000000a7"
CHAT = "00000000-0000-0000-0000-0000000000a4"
DAEMON = "benchmark-synthetic-daemon"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout.strip()


def psql(container, database, sql):
    proc = subprocess.run(["docker", "exec", "-i", container, "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1",
                           "-U", "bench", "-d", database], input=sql, text=True, capture_output=True)
    if proc.returncode:
        raise RuntimeError(f"synthetic SQL failed in {database}: {proc.stderr[-800:]}")


def wait_for(label, ready, proc=None, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"{label} exited with code {proc.returncode}")
        if ready():
            return
        time.sleep(0.25)
    raise RuntimeError(f"{label} did not become ready")


def jwt(secret):
    def encode(obj):
        return base64.urlsafe_b64encode(json.dumps(obj, separators=(",", ":")).encode()).decode().rstrip("=")
    now = int(time.time())
    unsigned = encode({"alg": "HS256", "typ": "JWT"}) + "." + encode({
        "sub": USER, "email": "benchmark@example.invalid", "name": "Benchmark User",
        "sid": "synthetic-session", "iat": now, "exp": now + 3600})
    signature = base64.urlsafe_b64encode(hmac.new(secret.encode(), unsigned.encode(), hashlib.sha256).digest()).decode().rstrip("=")
    return unsigned + "." + signature


def request(port, path, token, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method="POST",
                                 headers={"Authorization": "Bearer " + token,
                                          "X-Workspace-ID": WORKSPACE, "Content-Type": "application/json"})
    started = time.perf_counter_ns()
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    except OSError as error:
        return 0, {"transport_error": type(error).__name__}, (time.perf_counter_ns() - started) / 1_000_000
    with response:
        payload = response.read(2_000_000)
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        return response.status, json.loads(payload), elapsed_ms


def shape(value):
    if isinstance(value, dict):
        return {key: shape(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [shape(value[0])] if value else []
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    return type(value).__name__


def shape_diff_paths(a, b, path="$", limit=10):
    if a == b:
        return []
    if isinstance(a, dict) and isinstance(b, dict):
        paths = []
        for key in sorted(a.keys() | b.keys()):
            paths.extend(shape_diff_paths(a.get(key, "<absent>"), b.get(key, "<absent>"),
                                          path + "." + key, limit))
            if len(paths) >= limit:
                break
        return paths[:limit]
    if isinstance(a, list) and isinstance(b, list) and a and b:
        return shape_diff_paths(a[0], b[0], path + "[0]", limit)
    return [path]


def summary(samples):
    ordered = sorted(samples)
    if not ordered:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "mean_ms": None}
    return {"n": len(ordered), "p50_ms": ordered[math.ceil(len(ordered) * .5) - 1],
            "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1],
            "mean_ms": round(statistics.mean(ordered), 3)}


def seed(container, database):
    psql(container, database, f"""
        INSERT INTO "user" (id,name,email) VALUES ('{USER}','Benchmark User','benchmark@example.invalid');
        INSERT INTO workspace (id,name,slug) VALUES ('{WORKSPACE}','Benchmark Workspace','benchmark-workspace');
        INSERT INTO member (workspace_id,user_id,role) VALUES ('{WORKSPACE}','{USER}','owner');
        INSERT INTO agent_runtime (id,workspace_id,name,runtime_mode,provider,owner_id,status)
            VALUES ('{RUNTIME}','{WORKSPACE}','Benchmark Runtime','local','synthetic','{USER}','online');
        UPDATE agent_runtime SET last_seen_at=now() WHERE id='{RUNTIME}';
        INSERT INTO agent (id,workspace_id,name,runtime_mode,owner_id,runtime_id)
            VALUES ('{AGENT}','{WORKSPACE}','Benchmark Agent','local','{USER}','{RUNTIME}');
        INSERT INTO issue (id,workspace_id,title,description,creator_type,creator_id,status,priority,number,position)
            VALUES ('{ISSUE}','{WORKSPACE}','Benchmark Issue','Synthetic issue','member','{USER}','in_progress','none',1,0);
        INSERT INTO chat_session (id,workspace_id,agent_id,creator_id,title,status,explicitly_created_at)
            VALUES ('{CHAT}','{WORKSPACE}','{AGENT}','{USER}','Benchmark chat session','active',now());
    """)


def check_owner(container, owner):
    result = subprocess.run(["docker", "inspect", "-f", '{{index .Config.Labels "multica.benchmark.owner"}}', container],
                            capture_output=True, text=True)
    return result.returncode == 0 and result.stdout.strip() == owner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=100, help="samples per backend and scenario (default 100)")
    parser.add_argument("--rps", type=float, default=4., help="global request rate limit, at most 5 (default 4)")
    args = parser.parse_args()
    if args.samples < 1 or not 0 < args.rps <= 5:
        parser.error("samples must be positive and rps must be in (0,5]")
    owner = "benchmark-claim-" + uuid.uuid4().hex
    ports = []
    while len(ports) < 3:
        port = free_port()
        if port != 5432 and port not in ports:
            ports.append(port)
    pg_port, go_port, rust_port = ports
    report = {"revision": run(["git", "rev-parse", "HEAD"], cwd=REPO),
              "source_tree_note": "Binary digests identify exact compiled artifacts; HEAD alone does not describe uncommitted Go/Rust sources.",
              "binary_sha256": {},
              "samples_per_backend_scenario": args.samples, "max_global_rps": args.rps,
              "postgres_image": "postgres:17", "task_kind": ["synthetic_chat_session", "synthetic_issue"],
              "postgres_host_port": pg_port,
              "go_host_port": go_port, "rust_host_port": rust_port,
              "migrations": {"go": "server/migrations", "rust": ["server-rust/migrations", "server-rust/migrations_zbs"]},
              "scenarios": {}, "exit_codes": {}, "errors": [], "lab_compare": "Not used: existing lab.py fixture/case sets target different APIs, not synthetic claim polling; status and JSON shape parity are checked directly here."}
    container = go_container = ""
    processes = []
    try:
        with contextlib.nullcontext(tempfile.mkdtemp(prefix="benchmark-claim-", dir=HERE)) as tmp_name:
            tmp = Path(tmp_name)
            go_binary = tmp / "go-server"
            migrate_binary = tmp / "go-migrate"
            # Prefer the exact module toolchain, never a stale oracle binary.
            go_tool = Path.home() / "go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.9.linux-amd64/bin/go"
            if not go_tool.is_file():
                raise RuntimeError("Go 1.26.9 toolchain is not installed locally")
            go_build_env = dict(os.environ, GOTOOLCHAIN="local", CGO_ENABLED="0")
            for output, package in ((go_binary, "./cmd/server"), (migrate_binary, "./cmd/migrate")):
                print(f"Building current Go source: {package}", flush=True)
                with (tmp / (output.name + ".build.log")).open("w") as log:
                    result = subprocess.run([str(go_tool), "build", "-buildvcs=false", "-o", str(output), package], cwd=GO,
                                            env=go_build_env, stdout=log, stderr=subprocess.STDOUT)
                report["exit_codes"]["build_" + output.name] = result.returncode
                if result.returncode:
                    raise RuntimeError(f"Go build failed for {package}: {(tmp / (output.name + '.build.log')).read_text()[-1200:]}")
            cargo = Path.home() / ".cargo/bin/cargo"
            rust_binary = Path("/data/rust-build/zbstech/debug/multica-server")
            print("Building current Rust source", flush=True)
            with (tmp / "rust.build.log").open("w") as log:
                result = subprocess.run([str(cargo), "build", "--locked", "--offline", "--manifest-path", str(HERE / "Cargo.toml"),
                                         "--target-dir", str(rust_binary.parent.parent), "--bin", "multica-server"],
                                        cwd=HERE, stdout=log, stderr=subprocess.STDOUT)
            report["exit_codes"]["build_rust"] = result.returncode
            if result.returncode:
                raise RuntimeError("Rust build failed; see rust.build.log in temporary run directory")
            for name, binary in (("go_server", go_binary), ("go_migrate", migrate_binary), ("rust_server", rust_binary)):
                digest = hashlib.sha256()
                with binary.open("rb") as source:
                    for chunk in iter(lambda: source.read(1 << 20), b""):
                        digest.update(chunk)
                report["binary_sha256"][name] = digest.hexdigest()
            # Docker publishes only loopback ports; the Go process shares this
            # container's network namespace so its :PORT listener is not public.
            container = run(["docker", "run", "-d", "--label", f"multica.benchmark.owner={owner}",
                             "-e", "POSTGRES_USER=bench", "-e", "POSTGRES_PASSWORD=synthetic_only",
                             "-e", "POSTGRES_DB=multica_go_bench",
                             "-p", f"127.0.0.1:{pg_port}:5432", "-p", f"127.0.0.1:{go_port}:{go_port}", "postgres:17"])
            wait_for("PostgreSQL", lambda: subprocess.run(
                ["docker", "exec", container, "pg_isready", "-U", "bench", "-d", "multica_go_bench"],
                capture_output=True).returncode == 0)
            # Official postgres entrypoint briefly stops its bootstrap server
            # before starting the final postmaster; wait past that transition.
            time.sleep(2)
            wait_for("PostgreSQL final startup", lambda: subprocess.run(
                ["docker", "exec", container, "psql", "-U", "bench", "-d", "postgres", "-tAc", "select 1"],
                capture_output=True).returncode == 0)
            psql(container, "postgres", "CREATE DATABASE multica_rust_bench")
            go_url = f"postgres://bench:synthetic_only@127.0.0.1:{pg_port}/multica_go_bench?sslmode=disable"
            rust_url = f"postgres://bench:synthetic_only@127.0.0.1:{pg_port}/multica_rust_bench?sslmode=disable"
            # No inherited application secrets/provider URLs enter server or migration subprocesses.
            base_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp), "DO_NOT_TRACK": "1",
                        "APP_ENV": "development", "JWT_SECRET": secrets.token_urlsafe(48)}
            with (tmp / "go.migrate.log").open("w") as log:
                result = subprocess.run([str(migrate_binary), "up"], cwd=GO,
                                        env=dict(base_env, DATABASE_URL=go_url), stdout=log, stderr=subprocess.STDOUT)
            report["exit_codes"]["migrate_go"] = result.returncode
            if result.returncode:
                raise RuntimeError("Go migrations failed; see go.migrate.log in temporary run directory")
            with (tmp / "rust.migrate.log").open("w") as log:
                result = subprocess.run([str(rust_binary), "migrate-up"], cwd=HERE,
                                        env=dict(base_env, RUST_DATABASE_URL=rust_url,
                                                 RUST_DEV_DATABASE_CONFIRM="multica_rust_bench",
                                                 RUST_MIGRATIONS_ROOT=str(HERE)), stdout=log, stderr=subprocess.STDOUT)
            report["exit_codes"]["migrate_rust"] = result.returncode
            if result.returncode:
                raise RuntimeError("Rust migrations failed; see rust.migrate.log in temporary run directory")
            for db in ("multica_go_bench", "multica_rust_bench"):
                seed(container, db)
            # Go binary runs inside the same disposable network namespace; only
            # PostgreSQL and its loopback-only published server port are reachable.
            go_container = run(["docker", "run", "-d", "--network", f"container:{container}",
                                "--label", f"multica.benchmark.owner={owner}",
                                "-e", "DATABASE_URL=postgres://bench:synthetic_only@127.0.0.1:5432/multica_go_bench?sslmode=disable",
                                "-e", f"PORT={go_port}", "-e", f"JWT_SECRET={base_env['JWT_SECRET']}",
                                "-e", "DO_NOT_TRACK=1", "-e", "APP_ENV=development",
                                "-v", f"{go_binary}:/benchmark-go:ro", "postgres:17", "/benchmark-go"])
            with (tmp / "rust.server.log").open("w") as log:
                rust_proc = subprocess.Popen([str(rust_binary)], cwd=HERE,
                    env=dict(base_env, RUST_DATABASE_URL=rust_url, RUST_BIND_ADDR=f"127.0.0.1:{rust_port}"),
                    stdout=log, stderr=subprocess.STDOUT)
                processes.append(rust_proc)
                def healthy(port):
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                            return response.status == 200
                    except (urllib.error.URLError, TimeoutError, OSError):
                        return False
                wait_for("Go server", lambda: healthy(go_port) or (not check_owner(go_container, owner) and False), timeout=100)
                wait_for("Rust server", lambda: healthy(rust_port), rust_proc, timeout=100)
                token = jwt(base_env["JWT_SECRET"])
                routes = {"singular": f"/api/daemon/runtimes/{RUNTIME}/tasks/claim", "batch": "/api/daemon/tasks/claim"}
                bodies = {"singular": None, "batch": {"daemon_id": DAEMON, "runtime_ids": [RUNTIME], "max_tasks": 1}}
                next_request = time.monotonic()
                def measured(port, path, body, authorization=token):
                    nonlocal next_request
                    delay = next_request - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    next_request = max(next_request, time.monotonic()) + 1 / args.rps
                    return request(port, path, authorization, body)
                for _ in range(4):
                    measured(go_port, routes["singular"], None)
                    measured(rust_port, routes["singular"], None)
                    measured(go_port, routes["batch"], bodies["batch"])
                    measured(rust_port, routes["batch"], bodies["batch"])
                for scenario in ("empty", "unauthorized", "bad_request", "claimed", "claimed_issue"):
                    for route in ("singular", "batch"):
                        label = route + "_" + scenario
                        results = {"go": [], "rust": []}
                        status_counts = {"go": {}, "rust": {}}
                        shapes = {"go": None, "rust": None}
                        mismatches = 0
                        failed_examples = []
                        invalid_count = 0
                        for iteration in range(args.samples):
                            observations = {}
                            for backend, port, db in (("go", go_port, "multica_go_bench"),
                                                      ("rust", rust_port, "multica_rust_bench")):
                                task_id = None
                                if scenario in ("claimed", "claimed_issue"):
                                    task_id = str(uuid.uuid4())
                                    # Chat tasks skip the issue load entirely, so the
                                    # issue scenario is what covers daemon_claim_load's
                                    # issue row decoding (including issue.number).
                                    subject = "chat_session_id" if scenario == "claimed" else "issue_id"
                                    subject_id = CHAT if scenario == "claimed" else ISSUE
                                    psql(container, db, f"UPDATE agent_runtime SET status='online',last_seen_at=now() WHERE id='{RUNTIME}'; "
                                        f"INSERT INTO agent_task_queue (id,agent_id,runtime_id,{subject},status,priority) "
                                        f"VALUES ('{task_id}','{AGENT}','{RUNTIME}','{subject_id}','queued',0)")
                                auth = token if scenario != "unauthorized" else "invalid-synthetic-bearer"
                                path = routes[route]
                                body = bodies[route]
                                if scenario == "bad_request":
                                    if route == "singular":
                                        path = "/api/daemon/runtimes/not-a-uuid/tasks/claim"
                                    else:
                                        body = {"daemon_id": DAEMON, "runtime_ids": [RUNTIME], "max_tasks": -1}
                                status, payload, ms = measured(port, path, body, auth)
                                if status:
                                    results[backend].append(ms)
                                status_counts[backend][str(status)] = status_counts[backend].get(str(status), 0) + 1
                                observations[backend] = (status, shape(payload))
                                shapes[backend] = shape(payload)
                                expected = 200 if scenario in ("empty", "claimed", "claimed_issue") else (401 if scenario == "unauthorized" else 400)
                                tasks = payload.get("tasks") if route == "batch" else payload.get("task")
                                task = tasks[0] if route == "batch" and isinstance(tasks, list) and len(tasks) == 1 else tasks
                                if status != expected or (scenario in ("claimed", "claimed_issue") and (
                                        not isinstance(task, dict) or task.get("id") != task_id)) or (
                                        scenario == "empty" and tasks not in (None, [])):
                                    invalid_count += 1
                                    if len(failed_examples) < 3:
                                        detail = {"backend": backend, "iteration": iteration,
                                                  "actual_status": status, "expected_status": expected,
                                                  "response_shape": shape(payload)}
                                        if task_id:
                                            detail["task_status_after_request"] = run([
                                                "docker", "exec", container, "psql", "-X", "-U", "bench", "-d", db,
                                                "-tAc", f"SELECT status FROM agent_task_queue WHERE id='{task_id}'"])
                                        failed_examples.append(detail)
                                if task_id is not None:
                                    # An HTTP transport failure can occur AFTER the DB dispatch;
                                    # always restore queue cardinality before the next request.
                                    psql(container, db, f"DELETE FROM task_token WHERE task_id='{task_id}'; "
                                        f"DELETE FROM agent_task_queue WHERE id='{task_id}'")
                            if observations["go"] != observations["rust"]:
                                mismatches += 1
                        report["scenarios"][label] = {"status_counts": status_counts, "response_shape": shapes,
                                                      "status_or_shape_mismatches": mismatches,
                                                      "invalid_responses": invalid_count, "failed_examples": failed_examples,
                                                      "shape_diff_paths": shape_diff_paths(shapes["go"], shapes["rust"]),
                                                      "latency": {name: summary(times) for name, times in results.items()}}
                        latencies = report["scenarios"][label]["latency"]
                        print(f"{label}: Go p50/p95 {latencies['go']['p50_ms']}/{latencies['go']['p95_ms']} ms, "
                              f"Rust {latencies['rust']['p50_ms']}/{latencies['rust']['p95_ms']} ms; "
                              f"parity mismatches={mismatches}, invalid={invalid_count}", flush=True)
            report["success"] = all(row["status_or_shape_mismatches"] == 0 and row["invalid_responses"] == 0
                                    for row in report["scenarios"].values())
    except Exception as exc:
        report["success"] = False
        report["errors"].append(str(exc))
        print(f"Benchmark failed: {exc}", file=sys.stderr, flush=True)
        if 'tmp' in locals() and (tmp / 'rust.server.log').exists():
            for line in (tmp / 'rust.server.log').read_text().splitlines():
                if "panicked at" in line:
                    report["errors"].append(line[:240])
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            report["exit_codes"]["rust_server"] = proc.returncode
        for name, cid in (("go_server", go_container), ("postgres", container)):
            if cid and check_owner(cid, owner):
                subprocess.run(["docker", "stop", "-t", "5", cid], check=True, capture_output=True)
                if name == "go_server":
                    value = subprocess.run(["docker", "inspect", "-f", "{{.State.ExitCode}}", cid],
                                           capture_output=True, text=True).stdout.strip()
                    report["exit_codes"]["go_server_after_requested_stop"] = int(value)
                subprocess.run(["docker", "rm", cid], check=True, capture_output=True)
        if 'tmp' in locals() and tmp.is_dir():
            shutil.rmtree(tmp)
        report["exit_codes"]["benchmark"] = 0 if report["success"] else 1
        REPORT.write_text(json.dumps(report, indent=2) + "\n")
        print(f"Report: {REPORT} (success={report['success']})", flush=True)
    return 0 if report["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
