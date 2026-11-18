#!/usr/bin/env python3
"""Regenerate synthetic cron expectations with frozen Go service/cron.go.

Usage: python3 tests/issue_write_cron_oracle.py <frozen-go/server>
Never points at a running server or a database.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

expected = "bcd821561357755da1b18d6f664a1267bfd07ab2941af2e516202762ff7edff7"
source = Path(sys.argv[1]) / "internal/service/cron.go"
body = source.read_bytes()
assert hashlib.sha256(body).hexdigest() == expected, "frozen Go cron source changed"
assert body.startswith(b"package service\n"), "unexpected Go package header"
folder = Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix="issue_write_cron_oracle_", dir=folder) as scratch:
    scratch = Path(scratch)
    (scratch / "cron.go").write_bytes(body.replace(b"package service\n", b"package main\n", 1))
    (scratch / "main.go").write_bytes((folder / "issue_write_cron_oracle.go").read_bytes())
    env = os.environ.copy()
    env.update({"GOWORK": "off", "GOPROXY": "off", "GOTOOLCHAIN": "local"})
    output = subprocess.check_output(
        [os.environ.get("GO", "go"), "run", str(scratch / "main.go"), str(scratch / "cron.go")],
        cwd=source.parents[1],
        env=env,
        timeout=120,
    )
    (folder / "issue_write_cron_oracle.json").write_bytes(output)
    print(f"wrote {len(output)} bytes; frozen Go SHA-256 {expected}")
