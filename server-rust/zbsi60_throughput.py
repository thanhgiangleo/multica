#!/usr/bin/env python3
"""Concurrency sweep: how many requests/second can each backend actually serve?

`zbsi60_latency.py` is a closed loop at concurrency 1 with an rps cap. It
answers "how long does one request take when nothing else is happening" and
says nothing about sustained capacity. This harness answers the other question:
N concurrent keep-alive workers, no pacing, sweeping concurrency 1/8/32/64 and
reporting achieved requests/second together with p50/p95/p99 and failure counts.

Fairness rules, all of which the report records so a later reader can check them
instead of trusting this comment:

* Both backends run as containers on the same PostgreSQL 17 instance with the
  same `--cpus=2 --memory=1024m` budget. Comparing a cgroup-limited Go against
  an unlimited native Rust would measure the cgroup, not the language.
* The database is built by the same Go migrations + Rust `migrate-up` + the
  byte-identical `SEED` import from `zbsi60_latency.py`, so throughput and
  latency describe the same fixtures.
* Each case is the certified `lab.py` case, and before any load is applied the
  release binary must pass that case's assertions once — a fast wrong answer is
  not a result.
* Client saturation is a real confound when the load generator is Python on the
  same host. Requests are therefore driven by several forked processes (each
  with its own GIL), and every level records client CPU seconds and each
  backend's cgroup `usage_usec` delta. A level where the client, not the server,
  is the bottleneck is flagged `client_bound` instead of being reported as
  server capacity.

No production target, no real data, no provider call: the stack is disposable,
loopback-only and labelled with this process's owner.
"""
import argparse
import hashlib
import http.client
import json
import multiprocessing as mp
import os
import resource
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import run_merge_diff as merge
from zbsi60_latency import SEED

ROOT = Path(__file__).resolve().parent
LAB = ROOT / "tools/zbsi63-lab-v2"
sys.path.insert(0, str(LAB))
import lab  # noqa: E402  (same assertions the contract compare uses)
RUST_BIN = ("/data/rust-build/zbstech/.multica-rust-sprint-20261102/"
            "target-zbsi-60/release/multica-server")

# Certified GET cases only. Every one of these is in the differential set, so a
# throughput number here describes an endpoint whose answer was already compared
# against Go byte for byte. They span the shapes that mattered in the latency
# run: single-row read, second single-row read, a per-integration list, and a
# lookup that misses.
CASES = (
    ("read-issue", LAB / "cases.json", [], LAB / "synthetic-fixture.json"),
    ("read-agent", LAB / "cases.json", [], LAB / "synthetic-fixture.json"),
    ("telegram-list-disabled", LAB / "p1-cases.json", ["--group", "p1"],
     LAB / "synthetic-fixture.json"),
    ("missing-session", LAB / "cases.json", [], LAB / "synthetic-fixture.json"),
)


def load_case(case_id, cases_path):
    for case in json.loads(cases_path.read_text())["cases"]:
        if case["id"] == case_id:
            return case
    raise SystemExit(f"case {case_id} not found in {cases_path}")


def spec(case, variables, prefix):
    """Request path and headers for a certified case, mirroring lab.request.

    lab.request opens a fresh connection per call by design (one sample must not
    inherit another's socket state), which is exactly what a load generator must
    not do. The header rules are copied so the two harnesses send the same
    request; only the connection handling differs.
    """
    rendered = lab.render(case, variables)
    headers = {"Accept": "application/json", "Connection": "keep-alive"}
    if case.get("auth", True):
        persona = case.get("auth_as", "owner")
        token = os.environ.get(
            prefix + ("BEARER" if persona == "owner" else persona.upper() + "_BEARER"))
        if token:
            headers["Authorization"] = "Bearer " + token
    if case.get("workspace", True):
        headers["X-Workspace-ID"] = variables["WORKSPACE_ID"]
    return rendered["path"], headers


def _worker(host, port, path, headers, expected, warmup_s, duration_s, sink, lock):
    """One thread: sequential keep-alive requests for the whole window."""
    lats, failed, nbytes = [], 0, 0
    conn = None
    started = time.perf_counter()
    measure_from = started + warmup_s
    deadline = started + warmup_s + duration_s
    while True:
        if time.perf_counter() >= deadline:
            break
        try:
            if conn is None:
                conn = http.client.HTTPConnection(host, port, timeout=20)
            begin = time.perf_counter()
            conn.request("GET", path, headers=headers)
            response = conn.getresponse()
            body = response.read()
            elapsed_ms = (time.perf_counter() - begin) * 1000
            if response.status != expected:
                failed += 1
            elif begin >= measure_from:
                lats.append(elapsed_ms)
                nbytes += len(body)
        except Exception:  # transport failure: drop the socket and reconnect
            failed += 1
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass
    with lock:
        sink["lats"].extend(lats)
        sink["failures"] += failed
        sink["bytes"] += nbytes


def _process(host, port, path, headers, expected, warmup_s, duration_s, threads, out_path):
    """One generator process: `threads` in-flight requests, then one result.

    The result goes to a file rather than a `multiprocessing.Queue`: a queue
    payload of several thousand floats overflows the 64 KiB pipe buffer, and a
    feeder thread blocked on a full pipe deadlocks the parent if it joins
    before draining. A file has no such limit and cannot deadlock.
    """
    before = resource.getrusage(resource.RUSAGE_SELF)
    sink = {"lats": [], "failures": 0, "bytes": 0}
    lock = threading.Lock()
    workers = [threading.Thread(target=_worker, args=(
        host, port, path, headers, expected, warmup_s, duration_s, sink, lock))
        for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    after = resource.getrusage(resource.RUSAGE_SELF)
    cpu_s = ((after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime))
    Path(out_path).write_text(json.dumps(
        {"lats": sink["lats"], "failures": sink["failures"],
         "bytes": sink["bytes"], "cpu_s": cpu_s}))


def level(host, port, path, headers, expected, concurrency, warmup_s, duration_s):
    """Drive `concurrency` requests in flight from several forked processes.

    One process would serialise the generator behind a single GIL long before
    either backend saturated; separate processes each get their own.
    """
    processes = min(concurrency, 4)
    threads = -(-concurrency // processes)
    with tempfile.TemporaryDirectory(prefix="zbsi60-gen-") as tmp:
        out_paths = [str(Path(tmp) / f"gen-{i}.json") for i in range(processes)]
        children = [mp.Process(target=_process, args=(
            host, port, path, headers, expected, warmup_s, duration_s, threads,
            out_path)) for out_path in out_paths]
        started = time.perf_counter()
        for child in children:
            child.start()
        for child in children:
            child.join()
        wall = time.perf_counter() - started
        latencies, failures, nbytes, client_cpu = [], 0, 0, 0.0
        for i, (child, out_path) in enumerate(zip(children, out_paths)):
            if child.exitcode != 0 or not Path(out_path).exists():
                raise RuntimeError(
                    f"generator process {i} failed (exit {child.exitcode})")
            body = json.loads(Path(out_path).read_text())
            latencies.extend(body["lats"])
            failures += body["failures"]
            nbytes += body["bytes"]
            client_cpu += body["cpu_s"]
    return {"latencies": latencies, "failures": failures, "bytes": nbytes,
            "client_cpu_s": client_cpu, "wall_s": wall, "processes": processes,
            "threads_per_process": threads}


def cgroup_cpu_usec(container):
    """CPU time actually charged to the container, in microseconds."""
    try:
        out = subprocess.run(["docker", "exec", container, "cat",
                              "/sys/fs/cgroup/cpu.stat"],
                             capture_output=True, text=True, timeout=10)
    except Exception:
        return None
    if out.returncode != 0:
        return None
    for line in out.stdout.splitlines():
        key, _, value = line.partition(" ")
        if key == "usage_usec":
            return int(value)
    return None


def run_case_correctness(port, case, fixtures, prefix, extra, tag):
    """One ordinary lab.py pass: the release binary must satisfy the assertions."""
    target = ROOT / ".zbsi60-throughput" / f"check-{tag}-{case['id']}.json"
    if target.exists():
        target.unlink()
    cmd = [sys.executable, str(LAB / "lab.py"), "http",
           "--base", f"http://127.0.0.1:{port}",
           "--fixtures", str(fixtures),
           "--cases", str(CASES_LOOKUP[case["id"]][0]),
           "--case", case["id"], "--prefix", prefix,
           "--samples", "3", "--warmup", "1", "--rps", "20",
           "--out", str(target)] + extra
    proc = subprocess.run(cmd, cwd=str(ROOT), env=dict(os.environ),
                          capture_output=True, text=True)
    if proc.returncode != 0:
        return {"case": case["id"], "passed": False,
                "error": (proc.stderr or proc.stdout).strip()[-400:]}
    body = json.loads(target.read_text())
    return {"case": case["id"], "passed": bool(body.get("passed")),
            "status": body["cases"][0].get("status") if body.get("cases") else None,
            "contract_sha256": (body["cases"][0].get("contract_sha256")
                                if body.get("cases") else None),
            "n": body["successful_latency"]["n"]}


CASES_LOOKUP = {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-bin", default=RUST_BIN)
    parser.add_argument("--levels", default="1,8,32,64")
    parser.add_argument("--duration", type=float, default=6.0)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if Path(args.out).exists():
        raise SystemExit("output already exists; choose a new report path")
    levels = [int(x) for x in args.levels.split(",") if x.strip()]
    if not levels or any(x < 1 for x in levels):
        raise SystemExit("levels must be positive integers")

    merge.verify_binary(merge.ORACLE, merge.ORACLE_SHA256, "go oracle")
    merge.verify_binary(merge.MIGRATE, merge.MIGRATE_SHA256, "go migrate")
    if not Path(args.rust_bin).exists():
        raise SystemExit(f"rust binary not found at {args.rust_bin}")

    for case_id, cases_path, extra, fixtures in CASES:
        CASES_LOOKUP[case_id] = (cases_path, extra, fixtures)

    pg_port, go_port, rust_port = merge.free_port(), merge.free_port(), merge.free_port()
    assert len({pg_port, go_port, rust_port}) == 3
    assert 5432 not in (pg_port, go_port, rust_port)
    net = f"zbsi60-throughput-net-{int(time.time())}-{os.getpid()}"
    owner = f"zbsi60-throughput-{int(time.time())}-{os.getpid()}"
    # A killed run must still tear the stack down: SIGTERM otherwise ends the
    # interpreter without running `finally`, which is how three whole stacks
    # were left behind by earlier aborted attempts.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    cid = go_cid = rust_cid = ""
    started = []
    reports = []
    correctness = {"go": [], "rust": []}
    try:
        merge.run(["docker", "network", "create", "--label",
                   f"multica.test.owner={owner}", net])
        # Postgres stays on its own bridge network with 5432 published on
        # loopback only; it is infrastructure, not a measured backend.
        cid = merge.run(["docker", "run", "-d", "--network", net,
                         "--label", f"multica.test.owner={owner}",
                         "--memory=1536m", "--cpus=2",
                         "-e", "POSTGRES_USER=zbsi63",
                         "-e", "POSTGRES_PASSWORD=synthetic_zbsi63",
                         "-e", "POSTGRES_DB=multica_zbsi63go",
                         "-p", f"127.0.0.1:{pg_port}:5432",
                         "postgres:17"]).stdout.strip()
        ready = 0
        for _ in range(90):
            if subprocess.run(["docker", "exec", cid, "psql", "-q", "-U", "zbsi63",
                               "-d", "multica_zbsi63go", "-tAc", "select 1"],
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
        p = subprocess.run([merge.MIGRATE, "up"],
                           env=dict(os.environ, DATABASE_URL=go_url),
                           cwd=str(ROOT / ".test-migrations"),
                           capture_output=True, text=True)
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

        # Both backends share the host network namespace and dial postgres
        # through the loopback port published above. This is the only shape
        # that works: publishing a port from a container that borrows another
        # container's netns is rejected, and postgres:17 EXPOSEs only 5432, so
        # the backends cannot be reached through postgres. With --network host
        # each backend still gets its own cgroup, which is what the CPU budget
        # below is enforced against and where server_cpu_usec is read.
        common = ["--network", "host", "--label",
                  f"multica.test.owner={owner}", "--memory=1024m", "--cpus=2"]
        go_cid = merge.run(["docker", "run", "-d"] + common +
                           ["-e", f"DATABASE_URL={go_url}",
                            "-e", f"PORT={go_port}",
                            "-e", f"JWT_SECRET={merge.SECRET}",
                            "-e", "DO_NOT_TRACK=1",
                            "-v", f"{merge.ORACLE}:/oracle:ro",
                            "--entrypoint", "/bin/sh", "postgres:17",
                            "-c", "exec /oracle"]).stdout.strip()
        # The Rust server refuses any non-loopback bind (src/main.rs) and must
        # be exec'd with zero argv, otherwise it would parse the image CMD as a
        # mode argument.
        rust_cid = merge.run(["docker", "run", "-d"] + common +
                             ["-e", f"RUST_DATABASE_URL={rust_url}",
                              "-e", f"RUST_BIND_ADDR=127.0.0.1:{rust_port}",
                              "-e", f"JWT_SECRET={merge.SECRET}",
                              "-v", f"{args.rust_bin}:/rust:ro",
                              "--entrypoint", "/bin/sh", "postgres:17",
                              "-c", "exec /rust"]).stdout.strip()

        class Ghost:
            def poll(self):
                return None

        merge.wait_healthy(go_port, Ghost())
        try:
            merge.wait_healthy(rust_port, Ghost())
        except RuntimeError:
            logs = subprocess.run(["docker", "logs", rust_cid], capture_output=True, text=True)
            print("--- rust container stdout ---\n" + logs.stdout[-3000:], flush=True)
            print("--- rust container stderr ---\n" + logs.stderr[-3000:], flush=True)
            print("--- rust container state ---", flush=True)
            print(subprocess.run(["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}",
                                  rust_cid], capture_output=True, text=True).stdout, flush=True)
            raise
        print(f"both healthy (go {go_port}, rust {rust_port}) "
              f"in cgroups {go_cid[:12]}/{rust_cid[:12]}", flush=True)

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
            signature = base64.urlsafe_b64encode(
                hmac.new(merge.SECRET.encode(), unsigned.encode(),
                         hashlib.sha256).digest()).decode().rstrip("=")
            return unsigned + "." + signature

        credentials = {
            "LAB_BEARER": jwt("00000000-0000-0000-0000-0000000000a2",
                              "synthetic-zbsi63@example.invalid", "Synthetic User"),
            "RUST_LAB_BEARER": jwt("00000000-0000-0000-0000-0000000000a2",
                                   "synthetic-zbsi63@example.invalid", "Synthetic User"),
            "LAB_MEMBER_BEARER": jwt("00000000-0000-0000-0000-0000000000a8",
                                     "synthetic-other-member@example.invalid", "Other Member"),
            "RUST_LAB_MEMBER_BEARER": jwt("00000000-0000-0000-0000-0000000000a8",
                                          "synthetic-other-member@example.invalid",
                                          "Other Member"),
            "LAB_TASK_BEARER": task_token,
            "RUST_LAB_TASK_BEARER": task_token,
        }
        os.environ.update(credentials)
        (ROOT / ".zbsi60-throughput").mkdir(exist_ok=True)

        backends = (("go", go_cid, go_port, "LAB_"),
                    ("rust", rust_cid, rust_port, "RUST_LAB_"))
        for backend, container, port, prefix in backends:
            for case_id, cases_path, extra, fixtures in CASES:
                case = load_case(case_id, cases_path)
                correctness[backend].append(
                    run_case_correctness(port, case, fixtures, prefix, extra, backend))
            failed = [c for c in correctness[backend] if not c["passed"]]
            if failed:
                raise RuntimeError(
                    f"{backend} failed the contract on the release stack: {failed}")
            print(f"{backend} correctness ok on {len(correctness[backend])} cases",
                  flush=True)

        for case_id, cases_path, extra, fixtures in CASES:
            case = load_case(case_id, cases_path)
            variables = json.loads(fixtures.read_text())
            for level_c in levels:
                for backend, container, port, prefix in backends:
                    path, headers = spec(case, variables, prefix)
                    cpu_before = cgroup_cpu_usec(container)
                    result = level("127.0.0.1", port, path, headers,
                                   case["status"], level_c,
                                   args.warmup, args.duration)
                    cpu_after = cgroup_cpu_usec(container)
                    summary = lab.summarize(result["latencies"])
                    usec = ((cpu_after - cpu_before)
                            if cpu_before is not None and cpu_after is not None else None)
                    # The measured window is exactly args.duration by construction;
                    # every worker stops at the same deadline. The whole-run wall is
                    # kept separately so process start-up cost is not mistaken for
                    # throughput.
                    window = args.duration
                    # CPU counters span the whole generator lifetime, warm-up
                    # included, so they must be divided by that same span. Dividing
                    # by `window` alone produced percentages above the 2-CPU limit
                    # (e.g. 259%), which is physically impossible.
                    cpu_window = args.warmup + args.duration
                    row = {
                        "backend": backend, "case": case_id, "path": path,
                        "concurrency": level_c,
                        "n": summary["n"], "failures": result["failures"],
                        "rps": round(summary["n"] / window, 2) if window > 0 else None,
                        "p50_ms": summary.get("p50_ms"), "p95_ms": summary.get("p95_ms"),
                        "p99_ms": summary.get("p99_ms"), "max_ms": summary.get("max_ms"),
                        "bytes": result["bytes"],
                        "generator_processes": result["processes"],
                        "generator_threads_per_process": result["threads_per_process"],
                        "wall_s": round(result["wall_s"], 3),
                        "server_cpu_s": round(usec / 1e6, 3) if usec is not None else None,
                        "cpu_window_s": cpu_window,
                        "server_cpu_pct": (round(usec / 1e6 / cpu_window * 100, 1)
                                           if usec is not None else None),
                        "client_cpu_s": round(result["client_cpu_s"], 3),
                        "client_cpu_pct": round(result["client_cpu_s"] / cpu_window * 100, 1),
                    }
                    # A backend that had CPU left over did not set this row's
                    # ceiling; the generator did. Reported, not hidden.
                    row["server_headroom_pct"] = (
                        round(100 - row["server_cpu_pct"] / 2, 1)
                        if row["server_cpu_pct"] is not None else None)
                    row["client_bound"] = bool(
                        level_c >= 8 and row["server_headroom_pct"] is not None
                        and row["server_headroom_pct"] > 40)
                    reports.append(row)
                    print(f"{backend} {case_id} c={level_c} n={row['n']} "
                          f"rps={row['rps']} p95={row['p95_ms']} "
                          f"fail={row['failures']} srv_cpu={row['server_cpu_pct']}% "
                          f"cli_cpu={row['client_cpu_pct']}%", flush=True)
    finally:
        for container in (rust_cid, go_cid, cid):
            if not container:
                continue
            label = subprocess.run(
                ["docker", "inspect", "-f",
                 '{{index .Config.Labels "multica.test.owner"}}', container],
                capture_output=True, text=True).stdout.strip()
            if label == owner:
                subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        label = subprocess.run(
            ["docker", "network", "inspect", "-f",
             '{{index .Labels "multica.test.owner"}}', net],
            capture_output=True, text=True).stdout.strip()
        if label == owner:
            subprocess.run(["docker", "network", "rm", net], capture_output=True)

    paired = {}
    for row in reports:
        paired.setdefault((row["case"], row["concurrency"]), {})[row["backend"]] = row
    ratios = []
    for (case_id, level_c), pair in sorted(paired.items()):
        go, rust = pair.get("go"), pair.get("rust")
        if go and rust and go.get("rps") and rust.get("rps"):
            ratios.append({"case": case_id, "concurrency": level_c,
                           "rust_over_go_rps": round(rust["rps"] / go["rps"], 3)})
    Path(args.out).write_text(json.dumps({
        "harness": "zbsi60_throughput.py",
        "levels": levels,
        "duration_s": args.duration, "warmup_s": args.warmup,
        "client_processes_per_level": "min(concurrency, 4)",
        "rust_bin": {"path": args.rust_bin, "sha256": merge.file_sha256(args.rust_bin)},
        "oracle": {"path": merge.ORACLE, "sha256": merge.ORACLE_SHA256},
        "stack": {"database": "postgres:17", "postgres_limit": "1536m/2cpu",
                  "backend_limit": "1024m/2cpu each, identical",
                  "backend_network": "host namespace; postgres published on 127.0.0.1",
                  "case_selection": "certified differential GET cases only",
                  # Disclosed rather than hidden: the Go oracle hard-codes
                  # newMainHTTPServer(":"+PORT) and cannot be made loopback-only,
                  # so on a random high port it listens on all host interfaces
                  # for the lifetime of this run. The Rust server binds loopback.
                  "go_bind": "0.0.0.0:<random high port> for the run's duration"},
        "correctness": correctness,
        "warning": ("Closed loop at each level, so latency is what a client at that "
                    "concurrency sees (`server_cpu_pct` is relative to the 2-CPU limit, "
                    "`client_cpu_pct` to one core and may exceed 100 because the "
                    "generator is several processes). `client_bound` marks rows at "
                    "concurrency >= 8 where the backend still had more than 40% of its "
                    "limit idle: there the generator, not the server, set the ceiling. "
                    "Payloads are the small certified fixtures, so this measures fixed "
                    "per-request cost, not bulk serialisation."),
        "rows": reports, "rps_ratios": ratios,
    }, indent=1))
    print(f"wrote {args.out} with {len(reports)} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())