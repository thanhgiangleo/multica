#!/usr/bin/env python3
"""Package owned ZBSI-76 sources plus a separately reviewed shared wiring patch."""
import argparse
import difflib
import hashlib
import io
import json
import pathlib
import subprocess
import tarfile
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
EXPECTED_PIN = "364e1746763729ecd00b06d10ed1d9c925e5796eb0a965d42f49a7d7a655a381"
EXPECTED_GO = "cdf63dc829f7e935c03652e3bddbba4f5c91e044a4b36b089a72c658936bde57"
OWNED = (
    "src/api/files_ops_sprint.rs",
    "src/api/files_ops_sprint_local.rs",
    "src/api/files_ops_sprint_s3.rs",
    "src/api/files_ops_sprint_avatar.rs",
    "src/api/files_ops_sprint_feedback.rs",
    "src/api/files_ops_sprint_upload.rs",
    "src/api/files_ops_sprint_health.rs",
    "src/api/files_ops_sprint_cleanup.sql",
    "src/api/files_ops_sprint_user_assets.sql",
    "src/api/files_ops_sprint_user_assets_index.sql",
    "src/api/files_ops_sprint_user_assets_retention.sql",
    "src/api/files_ops_sprint_user_assets.rs",
    "tests/files_ops_sprint.rs",
    "tests/files_ops_sprint_verify.sh",
    "tests/files_ops_sprint_package.py",
)
SHARED = ("Cargo.toml", "Cargo.lock", "src/api.rs", "src/api/catalog.rs", "src/main.rs")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def archive_files(path):
    files = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            name = pathlib.PurePosixPath(member.name)
            if (
                name.is_absolute()
                or ".." in name.parts
                or member.name in files
                or not (member.isfile() or member.isdir())
            ):
                raise SystemExit("unsafe or duplicate archive entry")
            if member.isfile():
                files[member.name] = archive.extractfile(member).read()
    return files


def pack(path, files):
    with tarfile.open(path, "w:gz") as archive:
        for name, data in sorted(files.items()):
            info = tarfile.TarInfo(name)
            info.mode = 0o644
            info.uid = info.gid = info.mtime = 0
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def create(args):
    if digest(args.rust_pin.read_bytes()) != EXPECTED_PIN:
        raise SystemExit("Rust source pin SHA-256 mismatch")
    if digest(args.go_pin.read_bytes()) != EXPECTED_GO:
        raise SystemExit("Go source pin SHA-256 mismatch")
    before = archive_files(args.rust_pin)
    if len(before) != 1930:
        raise SystemExit("Rust pin file count mismatch")
    go = archive_files(args.go_pin)
    go_manifest = json.loads(before["baseline/source-manifest.json"])
    for entry in go_manifest["files"]:
        if digest(go["server/" + entry["path"]]) != entry["sha256"]:
            raise SystemExit("frozen Go manifest mismatch: " + entry["path"])
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    if evidence["issue"] != "ZBSI-76":
        raise SystemExit("wrong issue evidence")
    # Refuse to silently lose edits outside this domain's integration surface.
    for name, old in before.items():
        if name not in SHARED and (ROOT / name).read_bytes() != old:
            raise SystemExit("unowned source differs from pin: " + name)
    for folder in ("src", "tests"):
        for path in (ROOT / folder).rglob("*"):
            if path.is_file():
                name = path.relative_to(ROOT).as_posix()
                if name not in before and name not in OWNED:
                    raise SystemExit("unlisted source file: " + name)
    source = dict(before)
    patches = []
    for name in SHARED:
        new = (ROOT / name).read_bytes()
        patches.extend(difflib.unified_diff(
            source[name].decode().splitlines(keepends=True),
            new.decode().splitlines(keepends=True),
            fromfile=f"a/{name}", tofile=f"b/{name}",
        ))
        source[name] = new
    for name in OWNED:
        if name in before:
            raise SystemExit("owned path exists in pin: " + name)
        source[name] = (ROOT / name).read_bytes()
    patch = "".join(patches).encode()
    if not patch:
        raise SystemExit("missing shared patch")
    # Verify the actual patch, not only a manifest assembled from live files.
    with tempfile.TemporaryDirectory(prefix="files_ops_sprint-rebuild-", dir=ROOT) as temp:
        rebuild = pathlib.Path(temp)
        for name, data in before.items():
            path = rebuild / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        subprocess.run(["patch", "--batch", "-p1"], input=patch, cwd=rebuild, check=True)
        for name in OWNED:
            path = rebuild / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(source[name])
        actual = {p.relative_to(rebuild).as_posix(): p.read_bytes()
                  for p in rebuild.rglob("*") if p.is_file()}
        if actual != source:
            raise SystemExit("pin + owned delta + shared patch reconstruction mismatch")
    checkpoint = args.checkpoint
    artifact = ROOT / f"files_ops_sprint-checkpoint-{checkpoint}"
    full = ROOT / f"files_ops_sprint-full-source-{checkpoint}.tar.gz"
    delta = ROOT / f"files_ops_sprint-delta-{checkpoint}.tar.gz"
    if artifact.exists() or full.exists() or delta.exists():
        raise SystemExit("checkpoint exists; never overwrite delivered artifacts")
    manifest = {
        "schema": "files_ops_sprint_checkpoint_v2", "issue": "ZBSI-76",
        "rust_pin_sha256": EXPECTED_PIN, "go_pin_sha256": EXPECTED_GO,
        "rust_pin_entry_count": len(before),
        "go_source_manifest": "baseline/source-manifest.json",
        "go_verified_entries": len(go_manifest["files"]),
        "owned_files": list(OWNED), "shared_patch_files": list(SHARED),
        "deleted_files": [], "status": "PARTIAL",
        "reconstruction_verified": True,
        "full_source_files": [{"path": p, "sha256": digest(data), "size": len(data)}
                              for p, data in sorted(source.items())],
        "shared_patch_sha256": digest(patch),
    }
    reproduce = f"""# ZBSI-76 checkpoint {checkpoint} — PARTIAL

Rust base: v16, SHA-256 `{EXPECTED_PIN}`; {len(before)} files.
Frozen Go archive SHA-256 `{EXPECTED_GO}`; {len(go_manifest['files'])} manifest entries verified.
Full source: {len(source)} files. Narrow delta: {len(OWNED)} owned files, separate shared patch for {len(SHARED)} files, empty deletion list, evidence and logs. Pin + owned delta + patch reconstruction verified byte-for-byte.

Only ZBSI-63 reviews/merges shared Cargo/API/catalog/main wiring. Do not overlay a sibling full archive. Shared patch also makes opaque `ApiError` public so library avatar helpers have a usable return type; its fields remain private. No commit/push/PR, production changes, provider spend or personal-agent smoke.

## Reconstruct

Use NEW empty directories; first verify SHA-256 values in the attached checksums and manifest. Reject archive symlinks, absolute paths and traversal entries before extracting.

```sh
mkdir source-rebuilt
# Pinned v16 and checkpoint archives must be trusted/verified first.
tar -xzf server-rust-zbsi63-merged-20261102-v16.tar.gz -C source-rebuilt
tar -xzf files_ops_sprint-delta-{checkpoint}.tar.gz
cp files_ops_sprint-checkpoint-{checkpoint}/owned/src/api/files_ops_sprint* source-rebuilt/src/api/
cp files_ops_sprint-checkpoint-{checkpoint}/owned/tests/files_ops_sprint* source-rebuilt/tests/
(cd source-rebuilt && patch -p1 < ../files_ops_sprint-checkpoint-{checkpoint}/files_ops_sprint-shared.patch)
```

Hash every full-source entry against `files_ops_sprint-manifest.json`. No deletes. Logs are evidence only and are not part of full-source manifest.

## Verification

Rust toolchain 1.95.0. Supplied `sprint-gate.py` is mandatory. Run foreground from reconstructed source, gate in `../attachments/`; adjust only gate location if needed. Gate limits two build slots, one DB slot, memory threshold and per-issue target. Exit 75/124 is not pass. Log paths must be fresh.

```sh
export RUSTUP_TOOLCHAIN=1.95.0
python3 ../attachments/sprint-gate.py --issue ZBSI-76 --kind build --log ./files_ops_sprint-unit-recheck.log --wait-seconds 120 --timeout-seconds 450 -- /home/zbstech/.cargo/bin/cargo test --offline --locked --lib files_ops_sprint -- --nocapture
python3 ../attachments/sprint-gate.py --issue ZBSI-76 --kind build --log ./files_ops_sprint-compile-recheck.log --wait-seconds 120 --timeout-seconds 480 -- /home/zbstech/.cargo/bin/cargo test --offline --locked --test files_ops_sprint --no-run
python3 ../attachments/sprint-gate.py --issue ZBSI-76 --kind db --log ./files_ops_sprint-db-recheck.log --wait-seconds 120 --timeout-seconds 480 -- bash tests/files_ops_sprint_verify.sh
python3 ../attachments/sprint-gate.py --issue ZBSI-76 --kind build --log ./files_ops_sprint-clippy-recheck.log --wait-seconds 120 --timeout-seconds 450 -- /home/zbstech/.cargo/bin/cargo clippy --offline --locked --lib --test files_ops_sprint -- -D warnings
```

DB runner starts only owned disposable PostgreSQL 16, random loopback port, refuses host 5432, synthetic credentials, EXIT cleanup with ownership checks. Never use infra-postgres, production data or another workdir's containers. See evidence for exact executed results and failed attempts; a gate exit 75 does not verify DB behavior. The DB fixture selects its intended orphan deterministically alongside the separate revoked-owner orphan and models foreign-workspace ownership with a real issue to preserve the legacy FK. No combined full-suite parity claim.

## Contracts and remaining work

- Real human issue/comment/standalone workspace upload, private download/metadata/list/preview, HMAC signed-download TTL 60s, single Range (malformed, multi-range or duplicate headers return 416 with no-store rather than full bytes), issue/comment revision bumps, cleanup intents and reconciler. Comment-only upload derives its issue; upload/delete lock issue then live comment then attachment. Comment mutations publish full attachment/reaction projection only after commit. Legacy comment attachments without an issue and missing machine/chat/task/source-context policy stay fail-closed.
- Unix local reads, writes and cleanup use directory-fd anchored openat/mkdirat/linkat/unlinkat with O_NOFOLLOW on each component; immutable writes never replace an existing object. Existing storage roots and every object-key parent directory must be service-owned and mode 0700 (or stricter); reads reject non-private, foreign-owned or hard-linked object leaves; cleanup refuses symlink, hard-linked, non-private or foreign-owned object and sidecar inodes before any unlink (retaining the DB intent for operator repair); root symlinks are refused, new roots use mkdir mode 0700 without post-create chmod, and new object directories are created private. Revalidating the root FD on every access rejects post-construction permission changes. An existing shared child directory causes read/write/cleanup to fail without changing its permissions. Operators must tighten legacy root and child ownership/mode before enabling Rust storage; do not chmod existing directories automatically. Non-Unix fallback still requires an exclusively service-owned storage root.
- Empty S3 objects are verified using exactly one numeric `Content-Length: 0` on the provider HEAD response; reqwest's response-body length hint is not the stored object length for HEAD. Missing/malformed/duplicate/positive lengths are refused. Bounded Range GET also rejects duplicate `Content-Range` fields; synthetic fake-provider tests cover these cases. Disposable PostgreSQL HTTP fixtures confirm that a zero-size ledger cannot serve an existing nonempty local object. This is stricter than Go, not real-provider interoperability evidence.
- Avatar image serving now requires a positive raster ledger size and a complete size-verified object read before returning bytes (including S3 `Content-Range` total), not merely an over-read that inferred EOF. S3 GET/HEAD signs `Accept-Encoding: identity` and rejects transformed `Content-Encoding`, multipart range payloads, and inconsistent or duplicate `Content-Length` on bounded 206 reads. Fake-provider tests exercise signed wire identity, normal byte ranges and forged/transformed responses; disposable PostgreSQL HTTP tests poison user/workspace avatar ledger sizes and restore them. These are stricter than Go and need real-provider compatibility testing before enabling a bucket.
- Upload MIME fallback now mirrors Go's first-512-byte text signature for non-ASCII bytes and DEL while still rejecting C0 binary controls. Unit and disposable DB tests cover multilingual owner-only user assets, foreign-user denial and avatar exclusion; other Go MIME signatures are not claimed fully ported.\n- Text previews now refuse out-of-policy or stale attachment ledger sizes before serving bytes. The 2 MiB bound is checked before storage IO; in-bound previews use the same size-verified local/S3 read as downloads, then enforce exact byte count. Eligibility normalizes media-type case, surrounding whitespace and parameters, and path-bearing basenames with either slash separator like the frozen Go whitelist; PostgreSQL HTTP fixtures cover legacy mixed-case JSON and nested Go/Dockerfile names plus shorter/zero/oversize poisoned size rows. This does not add MIME parsing of text content or prove real provider behavior.
- Attachment and owner-only user asset downloads now verify the complete provider object size against the durable ledger on full AND partial responses before returning bytes: S3 checks `Content-Range` total, Unix local reads check the opened private inode metadata, and non-Unix local reads check the opened file metadata. A stale ledger no longer silently returns a successful truncated prefix. Fake S3 and PostgreSQL HTTP fixtures cover mismatched sizes; avatar reads also verify the exact ledger size. The backend's not-found mapping for object-store errors remains generic; no production provider was tested.
- Signed attachment URLs accept only the exact minted query wire form (`exp=<canonical decimal>&sig=<64 lowercase hex>[&dl=1]`), rejecting percent-encoded/reordered fields, alternate numeric and signature spellings, duplicates and suffixes before storage or DB reads. Successful attachment metadata/list responses carrying private URLs are `no-store` + `no-referrer`; signed download failures are also `no-store` + `no-referrer`, with no private bytes. Synthetic unit and disposable PostgreSQL HTTP tests cover normal and invalid forms. Avatar URL parsing rejects query, fragment, userinfo and non-HTTP schemes. Configured local object base URLs require a canonical HTTP(S) URL with no userinfo/query/fragment, so storage cannot mint ambiguous or malformed private object links. These are stricter than Go and not a parity waiver.
- Workspace and user standalone raster avatar HMAC matches frozen Go domain/vector. Attachment reads require a live owner issue and, where linked, a non-tombstoned comment; deleting an owner revokes new network reads (including signed URLs), and issue list hides tombstoned comments. SVG excluded; stored MIME checked. Current proxy bound 8 MiB, no-store, DB-backed workspace and user keys only: stricter than Go, NOT full parity.
- Integrator must call `accept_avatar_url_in_transaction(state, tx, raw, current, actor, target_workspace)` inside the same transaction that writes the target avatar row, holding its lock through commit; this serializes asset retirement and membership revocation. The transactional checker validates directly on the caller transaction (including with a one-connection pool) instead of checking out another DB connection; signed avatar URLs fail closed when object storage is absent. `accept_avatar_url` alone is only a preflight check and is NOT a safe write guard. Call `resolve_avatar_url` on DTO projections. User-owned assets cannot be rebound by other users; workspace assets require source membership and cannot be rebound to a different workspace. An unchanged value still rechecks authorization. User upload has durable intent/ready ledger and crash cleanup; avatar read is signed only. User assets, including non-images, have owner-only `/api/user-assets/{{id}}/download` with Range and forced attachment; raw `/uploads/users` remains private. Explicit owner-only retirement has a durable `deleting` marker, denies deletion while bound to any user/workspace/squad avatar (including signed projections), and retries storage failures. `retire_replaced_avatar` can retire only a caller-owned old asset after its replacement commits and all avatar references are removed; ZBSI-63 must wire the hook to avatar mutations. Automatic retention policy and operator assets remain missing; cleanup of stale intents/deleted users requires a real scheduler. All attachment/user reads and cleanup deletes now require a canonical object key matching the row owner and ID; poisoned rows retain the ledger for operator repair. Workspace `/uploads` redirects to authenticated download; never public workspace bytes.
- Standalone user uploads reuse their durable-intent DB connection for finalization instead of retaining one checkout while requesting another. User retirement releases its committed caller connection before invoking reconciliation. These remove two one-slot pool starvation paths without merging the autocommitted intent and finalizer transaction. The HTTP fixture adds a separate `max_size(1)` pool, five-second upload/delete bounds, object/ledger checks, and future cleanup timestamps for explicit user and attachment deletion reset. Check this checkpoint's evidence: compilation does not establish DB behavior, and an unavailable DB gate leaves these new regressions unexecuted.
- `CLEANUP_MIGRATION` now installs an idempotent AFTER DELETE trigger on attachment. Direct row deletion and existing issue/comment/workspace FK cascades enqueue the deleted row's workspace/object URL in the same DB transaction, preserving cleanup ownership after the parent is gone. Rollback removes the intent; the trigger performs no object IO. On conflict it recaptures OLD's authoritative URL/workspace and clears stale cooldown. Reconcilers still validate canonical owner/ID keys before touching storage. This adds no foreign keys or cascades and no scheduler; integrator must ship the trigger in a NEW numbered migration if a previous cleanup-table migration has already been applied, rather than editing applied migration checksums. Disposable DB tests cover trigger reapplication, rollback visibility across connections, comment/issue/workspace cascade cleanup, conflict repair, and direct attachment deletion. Historical deletes before this trigger are not backfilled; updates that detach attachments without deleting their rows are not covered.
- Cleanup reconcilers cool down failed storage deletions, poisoned ledger rows and referenced orphan avatars for five minutes using durable `cleanup_checked_at`, preventing hot retry loops across processes; `FOR UPDATE SKIP LOCKED` still permits other eligible rows. A fresh explicit user retirement or committed attachment delete clears an older checked timestamp for prompt retry. Disposable PostgreSQL tests cover skip after failure/poison and successful cleanup after simulated cooldown expiry. Operator repair of poisoned rows does not automatically reset cooldown. Scheduler wiring remains the integrator's responsibility.
- Feedback writes real rows, trims/validates body/context, holds optional workspace membership through commit (concurrent revocation wins or waits), serializes hourly ten-per-user cap with user-row lock, emits telemetry only after commit. Its decoder accepts valid JSON without Content-Type and maps malformed/oversize/wrong-type bodies to Go's 400 JSON error; absent/null optional string fields become Go-style empty values. Analytics event keeps the case-preserved trimmed kind (default general), while exported label normalizers lowercase and allow-list kind/platform, avoiding raw label cardinality. Missing context omits the metadata key; an empty stack is omitted. Metadata strips feedback route query/fragment before durable storage (auth codes and search tokens cannot be accidentally retained); oversized or duplicate client metadata headers, control characters and oversized URL are rejected before DB access. Missing telemetry sink returns 503. These privacy guards and membership check are stricter than frozen Go; integrator must inject real telemetry and apply the exported label normalizers.
- ZBSI-63 assigns numbered migration for `CLEANUP_MIGRATION`, `USER_ASSETS_MIGRATION`, separate `USER_ASSETS_INDEX_MIGRATION`, and `USER_ASSETS_RETENTION_MIGRATION`, connects real storage, attachment events/wakeup, feedback telemetry and cleanup schedulers (`reconcile_cleanup`, `reconcile_user_assets`). No no-op production sinks.
- S3-compatible private object transport is implemented with SigV4, immutable PUT, bounded verified Range GET and retryable DELETE; canonical root URLs ending in `/` work for virtual-hosted buckets, while path-style subpaths cannot end in `/`; endpoint and ledger URL prefixes reject percent-encoded, non-ASCII or repeated-slash segments to prevent ambiguous object paths. Optional `with_session_token` signs the STS security token for all S3 methods; fake-provider tests independently reconstruct SigV4 from received request bytes and reject tampered path, token and body. Source the token from the integrator's secret provider and rotate the store when credentials expire. It is not production-wired or provider-verified. Integrator must configure a private bucket, explicit region/credentials and canonical endpoint/object URL, and never expose raw object URLs through a public CDN. CloudFront/presign, chat/task/machine policies, legacy comment rows without issue ownership, auth logout/refresh/config (await ZBSI-63 handoff), production lifecycle wiring, differential parity and cutover evidence remain incomplete.

Realtime health requires live injected realtime + daemonws counters; absent/incomplete/failed collector yields 503. The c4 optional ConnectInfo extractor compile error is fixed. A configured metrics token requires Bearer; unconfigured token only permits direct loopback TCP without forwarding headers. The main shared patch enables ConnectInfo, but does not wire a collector, so the standalone process returns 503 rather than invented counters.

Repackage using this script with explicit `--rust-pin`, `--go-pin`, `--evidence` and a fresh `--checkpoint`; it refuses overwrite. A package is not verification by itself.
"""
    artifacts = {
        "files_ops_sprint-shared.patch": patch,
        "files_ops_sprint-manifest.json": (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode(),
        "files_ops_sprint-evidence.json": (json.dumps(evidence, ensure_ascii=False, indent=2) + "\n").encode(),
        "files_ops_sprint-deletes.json": b"[]\n",
        "files_ops_sprint-reproduce.md": reproduce.encode(),
    }
    for name in OWNED:
        artifacts["owned/" + name] = source[name]
    for check in evidence["checks"]:
        if check.get("log"):
            log = pathlib.Path(check["log"])
            if log.name != str(log):
                raise SystemExit("evidence log must be a source-root basename")
            artifacts["logs/" + log.name] = (ROOT / log).read_bytes()
    artifact.mkdir()
    for name, data in artifacts.items():
        path = artifact / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    pack(full, source)
    pack(delta, {artifact.name + "/" + name: data for name, data in artifacts.items()})
    if archive_files(full) != source:
        raise SystemExit("full source archive differs from manifest")
    checksums = {full.name: digest(full.read_bytes()), delta.name: digest(delta.read_bytes())}
    checksum_file = artifact / "files_ops_sprint-checksums.json"
    checksum_file.write_text(json.dumps(checksums, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"full": full.name, "delta": delta.name, "sha256": checksums,
                      "files": len(source), "owned": len(OWNED), "shared": len(SHARED)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-pin", type=pathlib.Path, required=True)
    parser.add_argument("--go-pin", type=pathlib.Path, required=True)
    parser.add_argument("--evidence", type=pathlib.Path, required=True)
    parser.add_argument("--checkpoint", default="c2", choices=[f"c{n}" for n in range(2, 100)])
    create(parser.parse_args())
