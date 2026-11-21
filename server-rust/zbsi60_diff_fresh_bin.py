#!/usr/bin/env python3
"""Run the differential suite against the binary built from the current tree.

`run_merge_diff.py` hard-codes RUST_BIN to the target-zbsi-100 artifact, which
predates the current source. This launcher overrides only that module constant
so the evidence certifies the freshly built target-zbsi-60 binary; the harness
itself is unmodified.
"""
import sys

import run_merge_diff as merge

merge.RUST_BIN = ("/data/rust-build/zbstech/.multica-rust-sprint-20261102/"
                  "target-zbsi-60/debug/multica-server")
sys.exit(merge.main())