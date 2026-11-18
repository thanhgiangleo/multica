#!/usr/bin/env python3
"""Bounded, implementation-neutral HTTP contracts and latency samples. Stdlib only."""
import argparse
import copy
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent
TEMPLATE = re.compile(r"\{\{([A-Z][A-Z0-9_]*)\}\}")
MAX_BODY = 2 * 1024 * 1024


class LabError(Exception):
    pass


def load_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    # Reports must not contain response bodies, headers, cookies or tokens.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(value, out, indent=2, allow_nan=False)
        out.write("\n")


def percentile(values, q):
    """Exact nearest-rank empirical percentile, not histogram interpolation."""
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(q * len(values)) - 1)]


def summarize(values):
    values = [float(x) for x in values]
    if any(not math.isfinite(x) or x < 0 for x in values):
        raise LabError("durations must be finite nonnegative milliseconds")
    return {"n": len(values), "p50_ms": percentile(values, .50),
            "p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99),
            "mean_ms": statistics.mean(values) if values else None,
            "max_ms": max(values) if values else None,
            "low_sample_warning": len(values) < 100}


def pointer(document, path):
    if path == "":
        return document
    if not path.startswith("/"):
        raise LabError("JSON pointer must start with /")
    value = document
    try:
        for part in path[1:].split("/"):
            part = part.replace("~1", "/").replace("~0", "~")
            value = value[int(part)] if isinstance(value, list) else value[part]
        return value
    except (KeyError, IndexError, TypeError, ValueError):
        raise LabError("missing JSON pointer " + path) from None


def render(value, variables):
    if isinstance(value, str):
        def replace(match):
            key = match[1]
            if key not in variables:
                raise LabError("missing fixture variable " + key)
            return str(variables[key])
        return TEMPLATE.sub(replace, value)
    if isinstance(value, dict):
        return {k: render(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [render(v, variables) for v in value]
    return value


def normalize(document, variables, masks):
    result = copy.deepcopy(document)
    for path in masks:
        value = pointer(result, path)  # A mask must never hide a missing key.
        if value is None:
            continue  # Preserve null vs string, missing vs present.
        if not isinstance(value, str):
            raise LabError("masked value must be timestamp string or null: " + path)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value):
            raise LabError("masked value is not an RFC3339 timestamp: " + path)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise LabError("masked value is not an RFC3339 timestamp: " + path) from None
        if parsed.utcoffset() is None or abs(parsed.utcoffset().total_seconds()) >= 24 * 60 * 60:
            raise LabError("masked value is not an RFC3339 timestamp: " + path)
        parts = path[1:].split("/")
        parent = result
        for part in parts[:-1]:
            part = part.replace("~1", "/").replace("~0", "~")
            parent = parent[int(part)] if isinstance(parent, list) else parent[part]
        last = parts[-1].replace("~1", "/").replace("~0", "~")
        parent[int(last) if isinstance(parent, list) else last] = "<timestamp>"
    symbols = {}
    for name, value in variables.items():
        if isinstance(value, str) and value and name.endswith("_ID"):
            if value in symbols and symbols[value] != "@" + name:
                raise LabError("fixture IDs must not alias each other")
            symbols[value] = "@" + name
    def walk(value):
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        if isinstance(value, list):
            return [walk(v) for v in value]  # Ordering is part of the contract.
        return symbols.get(value, value) if isinstance(value, str) else value
    return walk(result)


def difference_paths(a, b, path=""):
    # JSON has one number type: Go encodes integral float64 as 0, serde as 0.0.
    # Keep booleans distinct (bool subclasses int in Python), and compare values.
    if type(a) in (int, float) and type(b) in (int, float):
        return [] if a == b else [path or "/"]
    if type(a) is not type(b):
        return [path or "/"]
    if isinstance(a, dict):
        found = []
        for key in sorted(a.keys() | b.keys()):
            escaped = key.replace("~", "~0").replace("/", "~1")
            child = path + "/" + escaped
            found.extend([child] if key not in a or key not in b else difference_paths(a[key], b[key], child))
        return found
    if isinstance(a, list):
        if len(a) != len(b):
            return [path + "/length"]
        return [p for i, (x, y) in enumerate(zip(a, b)) for p in difference_paths(x, y, path + "/" + str(i))]
    return [] if a == b else [path or "/"]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_target(base, allow_remote=False, writes=False, isolated=False):
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        raise LabError("base URL must be HTTP(S), without embedded credentials")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise LabError("base URL must be an origin, without path/query/fragment")
    local = parsed.hostname in ("127.0.0.1", "localhost", "::1")
    if not local and (not allow_remote or parsed.scheme != "https"):
        raise LabError("remote reads require HTTPS and --allow-remote-read")
    if writes and (not local or not isolated):
        raise LabError("writes require a loopback target and --isolated-target; use a disposable environment")


def request(base, case, variables, prefix, timeout=15):
    case = render(case, variables)
    path = case["path"]
    if not path.startswith("/") or path.startswith("//") or "#" in path or any(x in path for x in ("\r", "\n")):
        raise LabError("case path must stay on the configured origin")
    headers = {"Accept": "application/json"}
    if case.get("auth", True):
        persona = case.get("auth_as", "owner")
        if persona == "owner":
            cookie = os.environ.get(prefix + "COOKIE")
            token = os.environ.get(prefix + "BEARER")
        else:
            cookie = None
            token = os.environ.get(prefix + persona.upper() + "_BEARER")
        if not cookie and not token:
            raise LabError("authenticated case needs a configured credential for " + persona)
        if cookie:
            headers["Cookie"] = cookie
        if token:
            headers["Authorization"] = "Bearer " + token
    if case.get("workspace", True):
        if "WORKSPACE_ID" not in variables:
            raise LabError("missing fixture variable WORKSPACE_ID")
        headers["X-Workspace-ID"] = variables["WORKSPACE_ID"]
    body = None
    if "body" in case:
        body = json.dumps(case["body"]).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base.rstrip("/") + path, data=body, headers=headers, method=case.get("method", "GET"))
    started = time.perf_counter()
    try:
        response = urllib.request.build_opener(NoRedirect()).open(req, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    except (urllib.error.URLError, TimeoutError, OSError):
        raise LabError("HTTP transport failed (details suppressed to protect credentials)") from None
    with response:
        status = response.code
        payload = response.read(MAX_BODY + 1)
        content_type = response.headers.get("Content-Type", "").split(";")[0]
    elapsed = (time.perf_counter() - started) * 1000
    if len(payload) > MAX_BODY:
        raise LabError("response exceeds 2 MiB cap")
    if 300 <= status < 400:
        raise LabError("redirect refused; verify API origin and credentials")
    if status == 204 and case.get("status") == 204 and not payload and not content_type:
        return status, None, elapsed
    if content_type != case.get("content_type", "application/json"):
        raise LabError("unexpected response content type")
    try:
        document = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise LabError("response is invalid JSON") from None
    return status, document, elapsed


def validate_cases(cases):
    ids = set()
    for case in cases:
        identifier = case.get("id", "")
        if not re.fullmatch(r"[a-z0-9-]+", identifier) or identifier in ids:
            raise LabError("case IDs must be unique kebab-case")
        ids.add(identifier)
        if type(case.get("status")) is not int:
            raise LabError("case needs one exact expected HTTP status: " + identifier)
        if case.get("method", "GET") not in ("GET", "POST", "PATCH", "PUT", "DELETE"):
            raise LabError("unsupported method: " + identifier)
        if case.get("auth_as", "owner") not in ("owner", "member", "task"):
            raise LabError("unsupported synthetic actor: " + identifier)
        if not case.get("auth", True) and "auth_as" in case:
            raise LabError("anonymous case cannot select an actor: " + identifier)
        for key in case:
            if key not in {"id", "group", "path", "method", "body", "auth", "auth_as", "workspace", "status", "required", "equals", "capture", "mask_timestamps", "content_type"}:
                raise LabError("unknown case key: " + key)
        if "path" not in case:
            raise LabError("case missing path: " + identifier)
    if not cases:
        raise LabError("empty case selection")


def check_response(case, status, document, variables):
    if status != case["status"]:
        raise LabError("unexpected HTTP status " + str(status) + "; expected " + str(case["status"]))
    for path in case.get("required", []):
        pointer(document, path)
    for path, value in render(case.get("equals", {}), variables).items():
        actual = pointer(document, path)
        if type(actual) is not type(value) or actual != value:
            raise LabError("assertion failed at " + path + " (values suppressed)")
    for name, path in case.get("capture", {}).items():
        value = pointer(document, path)
        if not isinstance(value, str) or not value:
            raise LabError("captured ID must be a nonempty string")
        variables[name] = value
    return normalize(document, variables, case.get("mask_timestamps", []))


def run_cases(base, cases, variables, prefix):
    records, normalized = [], {}
    variables = dict(variables)
    for case in cases:
        record = {"case": case["id"], "passed": False}
        try:
            status, document, elapsed = request(base, case, variables, prefix)
            value = check_response(case, status, document, variables)
            digest = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            record.update(passed=True, status=status, elapsed_ms=elapsed, contract_sha256=digest)
            normalized[case["id"]] = value
        except LabError as error:
            record["error"] = str(error)
        records.append(record)
        if not record["passed"]:
            # Later cases may depend on a capture; never silently skip them.
            break
    return {"passed": len(records) == len(cases) and all(x["passed"] for x in records),
            "selected": len(cases), "executed": len(records), "unexecuted": len(cases) - len(records),
            "cases": records}, normalized


def selected_cases(args):
    cases = load_json(args.cases)["cases"]
    if args.group:
        cases = [x for x in cases if x.get("group") in args.group]
    if args.command == "http":
        cases = [x for x in cases if x.get("id") == args.case]
    validate_cases(cases)
    writes = any(x.get("method", "GET") != "GET" for x in cases)
    if writes and not args.allow_writes:
        raise LabError("selected cases contain writes; require --allow-writes and --isolated-target")
    return cases, writes


def source_inventory(server_root):
    import subprocess
    root = Path(server_root).resolve()
    if not (root / "go.mod").is_file() or not (root / "internal" / "handler").is_dir():
        raise LabError("expected a Multica server source root")
    revision = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    files = sorted(p for p in root.rglob("*") if p.is_file() and not p.is_symlink() and p.suffix in (".go", ".sql") and ".git" not in p.parts)
    digest = hashlib.sha256()
    test_files = []
    test_names = []
    for file in files:
        rel = file.relative_to(root).as_posix()
        contents = file.read_bytes()
        digest.update(rel.encode() + b"\0" + hashlib.sha256(contents).digest())
        if file.name.endswith("_test.go"):
            text = contents.decode("utf-8")
            names = re.findall(r"(?m)^func ((?:Test|Fuzz|Benchmark)[A-Za-z0-9_]+)\s*\(", text)
            build_tags = re.findall(r"(?m)^//go:build (.+)$", text[:2048])
            test_files.append({"path": rel, "tests": names, "build_tags": build_tags})
            test_names.extend(names)
    return {"source_revision": revision, "source_tree_sha256": digest.hexdigest(),
            "source_file_count": len(files), "go_test_file_count": len(test_files),
            "top_level_test_count": len(test_names), "test_files": test_files,
            "warning": "Static top-level test names; subtests, test execution and deployed binary parity are not inferred. Tree hash includes local changes, not secrets or file bodies."}


def phase_report(path):
    # Group by task + attempt. Never concatenate wall clocks from different hosts.
    tasks, invalid, ignored = {}, 0, 0
    allowed = {"claimed", "prepare_started", "skills_ready", "environment_ready", "runtime_started", "first_output_received", "first_tool_use", "turn_completed", "finished"}
    for line in Path(path).read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            invalid += 1
            continue
        phase = row.get("task_phase")
        if phase not in allowed or not row.get("task_id"):
            ignored += 1
            continue
        key = (str(row["task_id"]), str(row.get("attempt", "")), str(row.get("time", ""))[:10])
        total = row.get("total_elapsed_ms")
        if type(total) not in (int, float) or not math.isfinite(total) or total < 0:
            invalid += 1
            continue
        if phase in tasks.setdefault(key, {}):
            raise LabError("duplicate phase per task/attempt; split logs by execution or add attempt field")
        tasks[key][phase] = total
    pairs = {"claim_to_prepare": ("claimed", "prepare_started"),
             "prepare_to_skills": ("prepare_started", "skills_ready"),
             "skills_to_environment": ("skills_ready", "environment_ready"),
             "environment_to_runtime": ("environment_ready", "runtime_started"),
             "runtime_to_first_output": ("runtime_started", "first_output_received"),
             "claim_to_first_output": ("claimed", "first_output_received")}
    metrics = {}
    for name, (start, end) in pairs.items():
        samples = [t[end] - t[start] for t in tasks.values() if start in t and end in t]
        metrics[name] = {**summarize(samples), "missing_pairs": len(tasks) - len(samples)}
    return {"source": "daemon_phase_logs", "executions": len(tasks), "invalid_lines": invalid,
            "ignored_lines": ignored, "metrics": metrics,
            "warning": "Logs may mix web/channel/issue tasks. Filter by source/provider outside this tool. No-output runs remain missing, not zero."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("contract", "compare", "http"):
        p = sub.add_parser(command)
        p.add_argument("--base", required=True)
        p.add_argument("--fixtures", required=True)
        p.add_argument("--cases", default=str(ROOT / "cases.json"))
        p.add_argument("--group", action="append")
        p.add_argument("--prefix", default="LAB_")
        p.add_argument("--out", required=True)
        p.add_argument("--allow-writes", action="store_true")
        p.add_argument("--isolated-target", action="store_true")
        p.add_argument("--allow-remote-read", action="store_true")
        if command == "compare":
            p.add_argument("--candidate", required=True)
            p.add_argument("--candidate-fixtures", required=True)
            p.add_argument("--candidate-prefix", default="RUST_LAB_")
        if command == "http":
            p.add_argument("--case", required=True)
            p.add_argument("--samples", type=int, default=100)
            p.add_argument("--warmup", type=int, default=5)
            p.add_argument("--rps", type=float, default=1)
    for command in ("phases", "browser"):
        p = sub.add_parser(command)
        p.add_argument("--input", required=True)
        p.add_argument("--out", required=True)
    p = sub.add_parser("inventory")
    p.add_argument("--server-root", required=True)
    p.add_argument("--out", required=True)
    args = parser.parse_args()
    if Path(args.out).exists():
        raise LabError("output already exists; choose a new report path")
    if args.command == "inventory":
        report = source_inventory(args.server_root)
        passed = report["top_level_test_count"] > 0
    elif args.command == "phases":
        report = phase_report(args.input)
        passed = report["executions"] > 0 and report["invalid_lines"] == 0
    elif args.command == "browser":
        rows = load_json(args.input)
        if not isinstance(rows, list):
            raise LabError("browser input must be an array")
        names = {"create_session_http", "send_http", "send_to_first_frame", "send_to_first_text"}
        report = {"source": "browser_monotonic_clock", "metrics": {}}
        for name in sorted(names):
            samples = [r["ms"] for r in rows if r.get("metric") == name and r.get("ok") is True]
            report["metrics"][name] = {**summarize(samples),
                "failed": sum(r.get("metric") == name and r.get("ok") is False for r in rows)}
        passed = bool(rows) and any(v["n"] for v in report["metrics"].values()) and all(v["failed"] == 0 for v in report["metrics"].values())
    else:
        cases, writes = selected_cases(args)
        validate_target(args.base, args.allow_remote_read, writes, args.isolated_target)
        variables = load_json(args.fixtures)
        if args.command == "http":
            matches = [x for x in cases if x["id"] == args.case]
            if len(matches) != 1:
                raise LabError("--case must select exactly one case")
            case = matches[0]
            if case.get("method", "GET") != "GET":
                raise LabError("HTTP sampling only permits GET; measure real sends with browser observer")
            if not 1 <= args.samples <= 10000 or not 0 <= args.warmup <= 100 or not 0 < args.rps <= 20:
                raise LabError("bounds: samples 1..10000, warmup 0..100, rps (0,20]")
            samples, failures, warmup_failures = [], 0, 0
            for i in range(args.samples + args.warmup):
                start = time.perf_counter()
                try:
                    status, document, elapsed = request(args.base, case, variables, args.prefix)
                    check_response(case, status, document, dict(variables))
                    if i >= args.warmup:
                        samples.append(elapsed)
                except LabError:
                    if i < args.warmup:
                        warmup_failures += 1
                    else:
                        failures += 1
                time.sleep(max(0, 1 / args.rps - (time.perf_counter() - start)))
            report = {"case": args.case, "source": "closed_loop_http_client", "concurrency": 1,
                      "target_rps_cap": args.rps, "attempts": args.samples, "failures": failures,
                      "warmup_failures": warmup_failures, "successful_latency": summarize(samples),
                      "warning": "Includes network/client overhead. Success-only latency; failures reported separately. Not saturation/load testing; coordinated omission applies."}
            passed = failures == 0 and warmup_failures == 0
        else:
            report, baseline = run_cases(args.base, cases, variables, args.prefix)
            passed = report["passed"]
            if args.command == "compare":
                validate_target(args.candidate, args.allow_remote_read, writes, args.isolated_target)
                candidate, values = run_cases(args.candidate, cases, load_json(args.candidate_fixtures), args.candidate_prefix)
                diffs = {key: difference_paths(baseline[key], values[key]) for key in baseline.keys() & values.keys()}
                diffs = {key: paths for key, paths in diffs.items() if paths}
                passed = passed and candidate["passed"] and not diffs
                report = {"passed": passed, "baseline": report, "candidate": candidate, "differences": diffs}
    report["passed"] = passed
    write_json(args.out, report)
    print(json.dumps({"passed": passed, "report": args.out}))
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (LabError, ValueError, OSError) as error:
        # Do not print exceptions containing HTTP requests, response bodies or env.
        print("backend-lab: " + (str(error) if isinstance(error, LabError) else "invalid input or inaccessible file"), file=sys.stderr)
        sys.exit(2)
