#!/usr/bin/env python3
"""Freeze an allowlisted source snapshot; never read env files or runtime data."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def source_files(root):
    # Only checked source types and explicit compile-time assets are eligible.
    assets = {"go.mod", "go.sum", "sqlc.yaml", "pkg/publicapi/v1/openapi.yaml",
              "internal/handler/attachment_text_preview.json", "internal/handler/reserved_slugs.json"}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root)
        if any(part.startswith(".") for part in rel.parts) or path.is_symlink():
            continue
        embedded = (str(rel).startswith("internal/service/builtin_") and path.suffix == ".md")
        if path.is_file() and (path.suffix in {".go", ".sql"} or rel.as_posix() in assets or embedded):
            yield rel.as_posix(), path.read_bytes()


def source_hash(files):
    h = hashlib.sha256()
    for name, body in files.items():
        if Path(name).suffix in {".go", ".sql"}:
            h.update(name.encode() + b"\0" + hashlib.sha256(body).digest())
    return h.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def freeze(root, live_root, out, archive):
    files = dict(source_files(root))
    if "cmd/server/router.go" not in files or "go.mod" not in files:
        raise ValueError("not a server source root")
    live = dict(source_files(live_root)) if live_root else files
    changed = [name for name in sorted(files.keys() | live.keys()) if files.get(name) != live.get(name)]
    migrations = {
        key: [Path(name).name.removesuffix(".up.sql") for name in files
              if name.startswith(directory + "/") and name.endswith(".up.sql")]
        for key, directory in [("upstream", "migrations"), ("zbs", "migrations_zbs")]
    }
    if not migrations["upstream"]:
        raise ValueError("empty upstream migration catalog")
    text = files["cmd/server/router.go"].decode()
    # Registration expressions, not unique/full paths: nested Route/Group prefixes
    # and plugin path constants still need semantic expansion during route porting.
    routes = []
    for match in re.finditer(r'\b(\w+)\.(Get|Post|Put|Patch|Delete|Head|Options)\(\s*([^,\n]+),\s*([^\n]+)', text):
        if match[1] != "r":  # cache.Get(ctx, hash) is not an HTTP registration.
            continue
        path_expression = match[3].strip()
        is_health = path_expression in {'"/health"', '"/readyz"', '"/healthz"'} and match[4].strip().startswith('health.')
        routes.append({"line": text.count("\n", 0, match.start()) + 1,
                       "method": match[2].upper(), "router": match[1],
                       "path_expression": path_expression, "handler_expression": match[4].strip(),
                       "state": "implemented_fake_tested" if is_health else "unimplemented",
                       "evidence": ["server-rust/tests/health.rs"] if is_health else []})
    domains = sorted({Path(name).stem for name in files
                      if name.startswith("internal/handler/") and name.endswith(".go")
                      and not name.endswith("_test.go")})
    manifest = {
        "schema_version": 1, "reference_commit": "e8ae72c9dfc1b4474b61a9087f51b33cbee6a048",
        "oracle_executable_sha256": "bafb4054144544df9de09f64bcd4aff4c60a7beced367acbab86adae9e00fe36",
        "oracle_image_id": "sha256:3f6835a1461c89cba7cbc28187f0a3e84aed1ff5c044168ec3098069b68a8c41",
        "staged_go_sql_sha256": source_hash(files), "live_go_sql_sha256": source_hash(live),
        "staged_vs_live_changed_files": changed,
        "source_hash_algorithm": "sorted relative Go/SQL path + NUL + raw SHA256(body), concatenated then SHA256",
        "note": "Source fingerprint is not proof that binary was built from it. Prior 0aaf2704 identifier uses an unspecified different algorithm; not reasserted as a verified tree hash.",
        "policy": "allowlisted source only; no environment, credentials, runtime files, DB contents or binaries",
        "files": [{"path": name, "sha256": digest(body), "bytes": len(body)} for name, body in files.items()],
        "upstream_migrations": len(migrations["upstream"]), "zbs_migrations": len(migrations["zbs"]),
        "http_method_registrations": len(routes),
        "sql_named_queries": sum(len(re.findall(rb"(?m)^-- name: ", body)) for name, body in files.items()
                                 if name.startswith("pkg/db/queries/") and name.endswith(".sql")),
    }
    out.mkdir(parents=True, exist_ok=True)
    write_json(out / "source-manifest.json", manifest)
    write_json(out / "health-migrations.json", migrations)
    write_json(out / "route-inventory.json", {"warning": "Raw router registrations, not fully expanded paths or parity evidence", "routes": routes})
    top_domains = ["foundation", "schema_migrations", "auth_cookie_pat_csrf", "workspace_isolation", "users_members", "workspaces_projects_issues_comments", "agents_skills", "chat_sessions_messages", "files_attachments", "tasks_claim_lease_terminal", "websocket_user", "websocket_daemon", "scheduler_wakeups_autopilots", "integrations_channels_webhooks", "notifications_billing", "storage_plugins", "operations"]
    write_json(out / "domain-tracker.json", {
        "domains": [{"name": name, "state": "partial_fake_tested" if name == "foundation" else "unimplemented",
                     "evidence": ["server-rust/tests/health.rs"] if name == "foundation" else []} for name in top_domains],
        "handler_files": [{"name": name, "state": "untriaged", "evidence": []} for name in domains],
        "warning": "Handler-file inventory is not proof of route or protocol parity; promote each domain only with independent evidence."
    })
    archive.parent.mkdir(parents=True, exist_ok=True)
    with archive.open("xb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as tar:
            for name, body in files.items():
                info = tarfile.TarInfo("server/" + name)
                info.size = len(body)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(body))
    write_json(out / "archive.json", {"filename": archive.name, "sha256": digest(archive.read_bytes()), "bytes": archive.stat().st_size})
    print(json.dumps({k: v for k, v in manifest.items() if k != "files"}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--compare-live-source", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    freeze(args.source, args.compare_live_source, args.out, args.archive)
