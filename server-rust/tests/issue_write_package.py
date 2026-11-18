#!/usr/bin/env python3
"""Package this child's cumulative owned delta; never overlay another child."""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--previous-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limits", type=Path)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    base = args.base.resolve()
    stem = args.output.resolve()
    previous = json.loads(args.previous_manifest.read_text())
    shared_names = set(previous["shared_files"])
    assert shared_names == {"Cargo.toml", "Cargo.lock", "src/api.rs", "src/api/catalog.rs"}
    prior_owned = set(previous["owned_files"])
    base_files = {p.relative_to(base).as_posix(): p for p in base.rglob("*") if p.is_file()}
    source_files = {}
    for p in source.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(source).as_posix()
        if p.suffix == ".log" or "__pycache__" in p.parts or "target" in p.parts:
            continue
        owned = rel in prior_owned or rel.startswith(("src/api/issue_write", "tests/issue_write", "baseline/issue_write"))
        if rel not in base_files and not owned:
            raise RuntimeError(f"unclassified non-source file: {rel}")
        source_files[rel] = p
    changed = {
        rel: p for rel, p in source_files.items()
        if rel not in base_files or p.read_bytes() != base_files[rel].read_bytes()
    }
    deleted = sorted(set(base_files) - set(source_files))
    assert not deleted, "review deletions explicitly before packaging"
    owned_files = {rel: p for rel, p in changed.items() if rel not in shared_names}
    unexpected = set(owned_files) - prior_owned
    assert all(p.startswith(("src/api/issue_write", "tests/issue_write", "baseline/issue_write")) for p in unexpected)
    shared_files = {rel: p for rel, p in changed.items() if rel in shared_names}
    patch = "".join(
        "".join(difflib.unified_diff(
            base_files[rel].read_text().splitlines(keepends=True),
            p.read_text().splitlines(keepends=True),
            fromfile=f"a/{rel}", tofile=f"b/{rel}",
        ))
        for rel, p in sorted(shared_files.items())
    )
    paths = {kind: Path(f"{stem}-{kind}{suffix}") for kind, suffix in {
        "checkpoint": ".tar.gz", "delta": ".tar.gz", "integration": ".patch",
        "logs": ".tar.gz", "manifest": ".json",
    }.items()}
    assert not any(p.exists() for p in paths.values()), "output exists; use a new output stem"
    paths["integration"].write_text(patch)
    with tarfile.open(paths["checkpoint"], "w:gz") as archive:
        for rel, p in sorted(source_files.items()):
            archive.add(p, arcname=f"server-rust/{rel}", recursive=False)
    with tarfile.open(paths["delta"], "w:gz") as archive:
        for rel, p in sorted(owned_files.items()):
            archive.add(p, arcname=f"owned/{rel}", recursive=False)
    logs = sorted(source.glob("*.log"))
    with tarfile.open(paths["logs"], "w:gz") as archive:
        for p in logs:
            archive.add(p, arcname=p.name, recursive=False)
    # Rebuild a clean source tree from the original baseline, owned delta and
    # separate integration patch. No child checkpoint is used as an overlay.
    with tempfile.TemporaryDirectory(prefix="issue-write-pr-replay-", dir=stem.parent) as directory:
        replay = Path(directory) / "server-rust"
        shutil.copytree(base, replay)
        subprocess.run(["git", "apply", str(paths["integration"])], cwd=replay, check=True)
        for rel, p in owned_files.items():
            destination = replay / rel
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(p, destination)
        replay_files = {p.relative_to(replay).as_posix(): p for p in replay.rglob("*") if p.is_file()}
        assert set(replay_files) == set(source_files)
        assert all(p.read_bytes() == replay_files[rel].read_bytes() for rel, p in source_files.items())
    manifest = {
        "issue": "ZBSI-66",
        "base_archive": previous["base_archive"],
        "base_sha256": previous["base_sha256"],
        "frozen_go_sha256": previous["frozen_go_sha256"],
        "previous_child_checkpoint_sha256": previous.get("checkpoint_sha256") or previous["artifacts"]["checkpoint"]["sha256"],
        "artifacts": {kind: {"file": p.name, "sha256": sha(p)} for kind, p in paths.items() if kind != "manifest"},
        "checkpoint_file_count": len(source_files),
        "owned_files": {rel: sha(p) for rel, p in sorted(owned_files.items())},
        "shared_files": {rel: sha(p) for rel, p in sorted(shared_files.items())},
        "deleted_files": deleted,
        "log_files": {p.name: sha(p) for p in logs},
        "replay": f"passed: {len(source_files)} files byte-identical from pinned baseline + {len(owned_files)} owned files + separate shared patch",
        "reproduce": "Extract original pinned Rust baseline into a new directory. From server-rust run git apply <integration.patch>, then copy owned/ from delta into server-rust/. Run bash tests/issue_write_verify.sh. Sprint gate mandatory; exit75/124 not pass.",
        "limits": args.limits.read_text().strip() if args.limits else previous["limits"],
    }
    paths["manifest"].write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"replay": manifest["replay"], "artifacts": {kind: str(p) for kind, p in paths.items()}}, indent=2))


if __name__ == "__main__":
    main()
