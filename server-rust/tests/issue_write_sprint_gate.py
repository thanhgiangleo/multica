#!/usr/bin/env python3
"""Bound Rust build/DB verification across isolated Multica worker checkouts."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--issue', required=True)
parser.add_argument('--kind', required=True, choices=['build', 'db'])
parser.add_argument('--log', required=True)
parser.add_argument('--wait-seconds', type=int, default=60)
parser.add_argument('--timeout-seconds', type=int, default=450)
parser.add_argument('command', nargs=argparse.REMAINDER)
args = parser.parse_args()
if not re.fullmatch(r'ZBSI-[1-9][0-9]*', args.issue):
    parser.error('issue must be real ZBSI-N key')
if not 0 <= args.wait_seconds <= 120 or not 1 <= args.timeout_seconds <= 480:
    parser.error('wait 0–120s, command timeout 1–480s')
command = args.command[1:] if args.command[:1] == ['--'] else args.command
if not command:
    parser.error('command required after --')
executable = Path(command[0]).name
if args.kind == 'build' and executable != 'cargo':
    parser.error('build kind requires cargo executable')
if args.kind == 'db' and executable != 'cargo':
    if executable not in ('bash', 'sh', 'python3') or len(command) < 2 or command[1].startswith('-'):
        parser.error('db kind requires cargo or bash/sh/python3 with a local test script, never multica')
    script = Path(command[1]).resolve()
    if not script.is_file() or not script.is_relative_to(Path.cwd().resolve()):
        parser.error('DB test script must exist inside current workdir')
log = Path(args.log).resolve()
if not log.is_relative_to(Path.cwd().resolve()):
    parser.error('log must be inside current workdir')
root = Path('/data/rust-build/zbstech/.multica-rust-sprint-20261102')
root.mkdir(mode=0o700, exist_ok=True)
if root.is_symlink() or root.stat().st_uid != os.getuid():
    parser.error('build gate root not owned by this worker')
root.chmod(0o700)


def mem_available_mib():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) // 1024
    return 0


def try_lock(filename):
    fd = os.open(root / filename, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    if os.fstat(fd).st_uid != os.getuid():
        os.close(fd)
        raise RuntimeError('build gate lock owned by another user')
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None


locks = []
try:
    deadline = time.monotonic() + args.wait_seconds
    while True:
        if mem_available_mib() < 6144:
            print('RESOURCE_WAIT: less than 6 GiB available; skip verification, no pass claimed', file=sys.stderr)
            sys.exit(75)
        if args.kind == 'db':
            db_fd = try_lock('db.lock')
            if db_fd is not None:
                locks.append(db_fd)
            else:
                if time.monotonic() >= deadline:
                    print('RESOURCE_WAIT: DB verification slot busy', file=sys.stderr)
                    sys.exit(75)
                time.sleep(2)
                continue
        for slot in ('compile-1.lock', 'compile-2.lock'):
            fd = try_lock(slot)
            if fd is not None:
                locks.append(fd)
                break
        if len(locks) == (2 if args.kind == 'db' else 1):
            break
        for fd in locks:
            os.close(fd)
        locks.clear()
        if time.monotonic() >= deadline:
            print('RESOURCE_WAIT: both Rust build slots busy', file=sys.stderr)
            sys.exit(75)
        time.sleep(2)
    env = os.environ.copy()
    env['CARGO_BUILD_JOBS'] = '1'
    env['CARGO_INCREMENTAL'] = '0'
    env['CARGO_PROFILE_DEV_DEBUG'] = '0'
    env['CARGO_PROFILE_TEST_DEBUG'] = '0'
    env['CARGO_TARGET_DIR'] = str(root / f'target-{args.issue.lower()}')
    with log.open('x') as output:
        proc = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            status = proc.wait(timeout=args.timeout_seconds)
        except subprocess.TimeoutExpired:
            # Never signal the workspace daemon, even if a test script started multica.
            try:
                daemon = subprocess.run(
                    ['multica', 'daemon', 'status', '--output', 'json'],
                    capture_output=True, text=True, timeout=15, check=True,
                )
                daemon_pid = int(json.loads(daemon.stdout)['pid'])
                if daemon_pid == proc.pid or os.getpgid(daemon_pid) == proc.pid:
                    raise RuntimeError('child process group includes workspace daemon')
            except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                print(f'TIMEOUT: refusing to signal unverified process group: {error}; waiting for natural exit', file=sys.stderr)
                proc.wait()
                sys.exit(124)
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            print(f'TIMEOUT: command exceeded {args.timeout_seconds}s; own child process stopped; see {log.name}', file=sys.stderr)
            sys.exit(124)
    print(f'EXIT {status}; log {log.name}; target {env["CARGO_TARGET_DIR"]}')
    sys.exit(status)
finally:
    for fd in locks:
        os.close(fd)
