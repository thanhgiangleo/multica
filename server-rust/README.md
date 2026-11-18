# Rust backend replacement (in progress)

This is **not** the `scripts/backend-lab/rust-candidate` mock. The server has
three health routes and eleven DB-backed authenticated GET registrations
(identity/workspace plus seven project/label/property/status catalog GETs).
Of 474 raw frozen Go router registrations, 460 remain unimplemented; nested
cloud runtime health routes are distinct and unimplemented. See `baseline/auth-read-inventory.md` for route scope, test
matrix and important auth/response differences. Do not deploy this service to production
or benchmark it against the Go backend as if it had functional parity.

## Checkpoint and reference

`baseline/route-inventory.json` is a raw router method-registration inventory;
router nesting and plugin path constants still require resolution. A method
registration is not a unique full route. `baseline/domain-tracker.json` records
17 coarse areas (foundation, migrations and several read/auth areas are partial),
plus a handler-file inventory that is not proof of domain parity.
`baseline/health-migrations.json` captures sorted version names for both
upstream (`schema_migrations`) and ZBS (`schema_migrations_zbs`). The SQL in
`migrations/` and `migrations_zbs/` was verified against the per-file SHA-256 in
`baseline/source-manifest.json` at this checkpoint, but the binary does not
verify hashes at runtime. `baseline/domain-tracker.json` records domain-level partial coverage without
claiming full API, security, migration or protocol parity.
`baseline/migration-evidence-20261031.json` records the initial isolated run results;
`baseline/migration-evidence-20261031-zbsi62.json` records the populated upgrade.

The separate `multica-go-source-20261031.tar.gz` issue attachment contains
3,830 allowlisted Go/SQL/module/embedded-asset source files, **not** any `.env`,
credentials, database contents or server executable. Do not extract blindly into
an existing checkout; inspect and compare before using it. Rebuild the freeze
only with access to the original staged tree:

```sh
python3 server-rust/tools/freeze_baseline.py \
  --source /path/to/staged/server --compare-live-source /path/to/live/server \
  --out server-rust/baseline --archive /path/to/new-checkpoint.tar.gz
```

The source-tree Go/SQL SHA-256 is `6408d883891648633d0c04478e58a9cbfaa02266d4abaada424d5550ea6a7056`;
the live tree differs only in `internal/daemon/gc_test.go` (live SHA-256
`fb5de2c1b0a7d469349a987fb587ab088ce16cb73398876bbfcc4ae6f7685bd4`).
The previously published staged-source `0aaf...` hash used a different or
undocumented algorithm; it is not interchangeable with this manifest hash.
The deployed `/app/server` SHA-256 and image ID were checked before freezing;
this does **not** prove the binary was built from either source tree.

## Managed development verification (2026-10-31)

A task-labelled Docker bridge allocated successfully from the default pool. The host
port 5432 belongs to an unrelated `infra-postgres`; its credential does not match
the stale `azox_multica-225` manifest, which belongs to another workdir. Neither
was modified or destroyed. On this checkout, a separate PostgreSQL container
and bridge bound only to 127.0.0.1:55432; `make up C=api` completed Go
migrations and API readiness, and `make status` reported the API running.
`make destroy ARGS=--yes` then completed on the isolated registry and database;
the task-owned container, volume and network were removed, and the original
`.env.worktree` restored. See `baseline/managed-dev-gate-20261031.json`.
This validates the managed Go dev workflow on a disposable PostgreSQL, not
Rust API parity or use of the unrelated DB on port 5432. The separate Rust
fresh/historical upgrade evidence is described above; full historical down
remains untested. Docker default-pool availability can change with other
host workloads; do not delete unidentified networks.

## Migration runner scope and safety

`src/migrate.rs` applies the upstream stream before ZBS on up, and reverses
that order on down. Each stream takes the Go runner's session advisory lock;
ordinary files execute with ledger writes in a transaction, while concurrent
index DDL executes without one. Invalid concurrent indexes are cleaned before
retry, and conditional skips still get ledger rows. Index hook maps are frozen
in `baseline/concurrent-index-hooks.json`. The CLI supports `migrate-up` and
`migrate-down`; it requires an exact `RUST_DEV_DATABASE_CONFIRM` matching a
`multica_*` database. That name check is *not* proof of isolation. The same
loopback-only database URL restriction applies to the HTTP service.

The three staged-Go pre-migration hooks are now ported: 103 walks UTC monthly
usage slices under advisory lock 4246, stamping the DB-time watermark only after
success; 198 reconciles violating attribution in locked batches; 431 walks all
chat IDs in 1000-row pages, retaining provenance of channel sessions. A failed
page does not record the migration; completed pages are idempotent on retry.
Versioned SQL and both ledgers remain unchanged. Synthetic historical data was
inserted at schema 102, 190, and 430 before the subsequent Rust upgrades.

This does **not** establish a production migration gate. Managed `make up`
was initially blocked by Docker address pool allocation and an unrelated stale
manifest, but a later isolated `make up C=api` / `make status` / `make destroy`
gate passed (see above). Neither the unrelated host database on port 5432 nor
the other checkout was modified. Full rollback of populated historical data,
production deployment, and auth/read parity are not verified. Legacy fork versions in the upstream ledger still require the Go
reference's separately reviewed `scripts/zbs/convert-legacy-fork-schema.sql`
procedure; the Rust runner refuses to guess a conversion.

For a fresh disposable database whose identity has been checked, from the repo
root:

```sh
export RUST_DATABASE_URL='postgres://<dev-user>:<dev-password>@127.0.0.1:<dev-port>/multica_<disposable-name>'
export RUST_DEV_DATABASE_CONFIRM='multica_<disposable-name>'
export RUST_MIGRATIONS_ROOT="$PWD/server-rust"
cargo run --manifest-path server-rust/Cargo.toml --locked --offline -- migrate-up
cargo run --manifest-path server-rust/Cargo.toml --locked --offline -- migrate-up # idempotency
# The down operation removes the disposable schema; only run on an owned empty lab DB.
```

Prefer the checkout's managed dev environment (`make up`/`make destroy`),
and explicitly verify ownership before supplying the DB URL. The earlier
`make up --ephemeral` attempt failed to allocate a Docker network, but the
later isolated managed Go gate passed. Synthetic Rust API tests used an
individually owned loopback-only PostgreSQL container. Never use a loopback
tunnel to a production database.

## Run checks

```sh
cargo test --manifest-path server-rust/Cargo.toml --locked --offline
cargo clippy --manifest-path server-rust/Cargo.toml --locked --offline --all-targets -- -D warnings
cargo build --release --manifest-path server-rust/Cargo.toml --locked --offline
rustfmt --check --edition 2024 server-rust/src/*.rs server-rust/tests/*.rs
(cd server && go test ./cmd/migrate -run 'TestEveryConcurrentUpBuildHasCleanup|TestEveryConcurrentDownBuildHasCleanup|TestEveryZBSConcurrentBuildHasCleanup|TestEveryPGBigmConcurrentDownBuildHasCleanup' -count=1)
```

The DB tests are ignored by default; only run them on a disposable database
verified to be owned by this test run (their fixture requires the exact DB name
`multica_zbsi62` and host `127.0.0.1`):

```sh
RUST_TEST_DATABASE_URL='postgres://<dev-user>:<dev-password>@127.0.0.1:<dev-port>/multica_zbsi62' \
  cargo test --manifest-path server-rust/Cargo.toml --locked --offline --test migrations -- --ignored
```

The three HTTP health routes use fake DB tests and are **not** evidence of
migration or auth parity. Real agent-account smoke tests require separate
authorization. `RUST_BIND_ADDR` defaults to `127.0.0.1:18091`; no `.env` is
loaded. The managed Go disposable dev DB gate passed in ZBSI-62; a partial Rust identity/workspace/catalog read slice now passes on a separate
disposable DB, and 22 synthetic authenticated request/response comparisons
matched a Go binary rebuilt from frozen staged source. This is not the deployed
Go executable, full parity, or a performance benchmark. See
`baseline/catalog-parity-20261031.json`. Safe-method browser JWT cookie
renewal and both CSRF cookies were added after those comparisons; CloudFront
signing is absent and a configured key-pair ID fails startup. Full rollback
and production auth/reads remain unverified. Historical-upgrade test requires an
**empty owned** `multica_zbsi62_upgrade` database and
`RUST_UPGRADE_DATABASE_URL` pointing at it; it intentionally leaves the
database for independent inspection and explicit cleanup. Synthetic failure
fixtures are in `tests/data_hooks.rs` and require `multica_zbsi62`.

## Account-level read checkpoint (2026-11-01)

The user-scoped GETs `/api/tokens/`, `/api/invitations`, and
`/api/invitations/{id}` now read migrated PostgreSQL. `tests/account.rs` is
ignored unless run with an isolated loopback PostgreSQL 16 named
`multica_zbsi63` and both migration streams applied; it checks credential
metadata redaction, cross-user invitations and failure/empty behavior.
The source reference is the verified staged Go archive, not the clean HEAD.
There are 14 DB-tested partial-parity GET registrations and 457 router
registrations still missing. See `baseline/account-read-20261101.json`; no
Go/Rust differential verification was run for these routes.
