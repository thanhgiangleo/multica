#!/usr/bin/env python3
"""Package owned ZBSI-66 source against the original pinned Rust baseline."""
import argparse
import difflib
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

BASE_SHA = 'd4506c153b2c9d7b9208beaf45498f8b8dd216ece064cda74076979268ab210f'
PREVIOUS_SHA = '0fac5be36f0027b356b201fb83253f030af85438055337b6770dd89ac9c1dc69'
SHARED = {'Cargo.toml', 'Cargo.lock', 'src/api.rs', 'src/api/catalog.rs'}
PREFIX = 'issue-write-move-snapshot-20261103'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def snapshot(path, checksum):
    assert sha(path.read_bytes()) == checksum, f'checksum mismatch: {path.name}'
    files = {}
    with tarfile.open(path) as archive:
        for item in archive:
            parts = Path(item.name).parts
            assert parts and parts[0] == 'server-rust' and '..' not in parts
            assert not Path(item.name).is_absolute() and (item.isfile() or item.isdir())
            if item.isfile():
                name = '/'.join(parts[1:])
                assert name not in files
                files[name] = archive.extractfile(item).read()
    return files


def owned(name):
    parts = Path(name).parts
    return (len(parts) > 2 and parts[:2] == ('src', 'api') and parts[2].startswith('issue_write')) or (
        len(parts) > 1 and parts[0] in ('tests', 'baseline') and parts[1].startswith('issue_write'))


def archive(path, files):
    with path.open('xb') as output, gzip.GzipFile(fileobj=output, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as tar:
            for name, data in sorted(files.items()):
                item = tarfile.TarInfo(name)
                item.size, item.mode = len(data), 0o644
                tar.addfile(item, io.BytesIO(data))


parser = argparse.ArgumentParser()
parser.add_argument('--base', type=Path, required=True)
parser.add_argument('--previous', type=Path, required=True)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
base = snapshot(args.base, BASE_SHA)
previous = snapshot(args.previous, PREVIOUS_SHA)
current = {name: (root / name).read_bytes() for name in previous}
for folder in ('src/api', 'tests', 'baseline'):
    for path in (root / folder).rglob('*'):
        if path.is_file() and owned(str(path.relative_to(root))):
            current[str(path.relative_to(root))] = path.read_bytes()
changed = {name: data for name, data in current.items() if base.get(name) != data}
assert all(owned(name) or name in SHARED for name in changed), 'unowned source change'
assert not set(base) - set(current), 'unexpected base deletion'
owned_files = {name: data for name, data in changed.items() if owned(name)}
shared_files = {name: data for name, data in changed.items() if name in SHARED}
out = args.out.resolve()
out.mkdir(parents=True, exist_ok=False)
patch = ''.join(''.join(difflib.unified_diff(
    base.get(name, b'').decode().splitlines(keepends=True), data.decode().splitlines(keepends=True),
    fromfile='a/' + name, tofile='b/' + name)) for name, data in sorted(shared_files.items()))
patch_path = out / f'{PREFIX}-integration.patch'
patch_path.write_text(patch)

# Rebuild exactly as the integrator will, without extracting a whole child archive.
with tempfile.TemporaryDirectory(prefix='issue_write_replay_', dir=out) as temp:
    replay = Path(temp) / 'server-rust'
    for name, data in base.items():
        target = replay / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    subprocess.run(['git', 'apply', str(patch_path)], cwd=replay, check=True)
    for name, data in owned_files.items():
        target = replay / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    rebuilt = {str(p.relative_to(replay)): p.read_bytes() for p in replay.rglob('*') if p.is_file()}
    assert rebuilt == current, 'replay is not byte-identical'

checkpoint = out / f'{PREFIX}-checkpoint.tar.gz'
delta = out / f'{PREFIX}-delta.tar.gz'
logs = out / f'{PREFIX}-logs.tar.gz'
archive(checkpoint, {'server-rust/' + name: data for name, data in current.items()})
archive(delta, {'owned/' + name: data for name, data in owned_files.items()})
log_files = sorted(p for p in root.glob('issue_write*.log') if p.is_file())
archive(logs, {path.name: path.read_bytes() for path in log_files})
manifest = {
    'issue': 'ZBSI-66', 'base_archive': args.base.name, 'base_sha256': BASE_SHA,
    'previous_child_checkpoint_sha256': PREVIOUS_SHA,
    'frozen_go_sha256': 'cdf63dc829f7e935c03652e3bddbba4f5c91e044a4b36b089a72c658936bde57',
    'artifacts': {name: {'file': path.name, 'sha256': sha(path.read_bytes())}
                  for name, path in [('checkpoint', checkpoint), ('delta', delta), ('integration', patch_path), ('logs', logs)]},
    'owned_files': {name: sha(data) for name, data in sorted(owned_files.items())},
    'shared_files': {name: sha(data) for name, data in sorted(shared_files.items())},
    'deleted_files': [], 'checkpoint_file_count': len(current),
    'turn_changed_files': sorted(name for name, data in current.items() if previous.get(name) != data),
    'replay': f'passed: {len(current)} files byte-identical from pinned baseline + {len(owned_files)} owned files + shared patch',
    'reproduce': 'Extract pinned base into a NEW directory. From server-rust apply integration.patch, copy owned/ from delta into server-rust/. Run bash tests/issue_write_verify.sh. Run go run tests/issue_write_move_oracle.go. Gate exit 75/124 is not pass.',
    'limits': 'partial/in_progress: see baseline/issue_write_move_snapshot_handoff.md and baseline/issue_write_move_snapshot_evidence.json; no production registration, real event/runtime adapters, full HTTP differential or mixed Go/Rust race proof',
    'log_files': [path.name for path in log_files],
}
(out / f'{PREFIX}-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
for name in ('issue_write_move_snapshot_handoff.md', 'issue_write_move_snapshot_evidence.json'):
    shutil.copyfile(root / 'baseline' / name, out / name)
print(json.dumps({'output': str(out), 'checkpoint_sha256': manifest['artifacts']['checkpoint']['sha256'],
                  'replay': manifest['replay'], 'owned_files': len(owned_files)}, indent=2))
