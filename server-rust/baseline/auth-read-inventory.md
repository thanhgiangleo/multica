# Auth/read route checkpoint (ZBSI-63, 2026-10-31)

Frozen reference: `multica-go-source-20261031.tar.gz` from ZBSI-60; all
3,830 files matched `source-manifest.json` (path, byte count, SHA-256).
`route-inventory.json` is the source registration list (474 entries), not
fully expanded URL patterns. Fourteen DB-backed GET registrations are implemented/tested on isolated fixtures;
three public health registrations have fake/health tests, and 457 registrations
remain unimplemented (including distinct nested cloud health paths).

| Router group / registration | Auth and DB dependencies | Current status |
| --- | --- | --- |
| public `/health`, `/readyz`, `/healthz` | no auth; readiness checks both migration ledgers | existing foundation |
| public `/auth/*`, `/api/config`, share-link previews, webhooks | OTP/OAuth/session mint, rate limits, signature/token gates and DB | unimplemented |
| daemon `/api/daemon/*` | distinct DaemonAuth (PAT, daemon token, cloud), workspace binding | unimplemented; never share human auth gate |
| plugin `/api/v1/*` | plugin bearer, installation scopes, rate limits | unimplemented |
| authenticated `/api/me` GET | JWT/PAT; `user`, ZBS shadow account policy | implemented, partial auth parity |
| authenticated `/api/workspaces/` GET | JWT/PAT; `member` JOIN `workspace` ordered created_at | implemented, no pagination in Go |
| authenticated `/api/workspaces/{id}/` GET | JWT/PAT; URL membership; `workspace` | implemented, 404 for nonmember |
| authenticated `/api/workspaces/{id}/members` GET | JWT/PAT; URL membership; `member` JOIN `user` ordered created_at | implemented, no pagination in Go |
| authenticated `/api/projects/` and `/api/projects/{id}/` GET | membership, filters, project issue/resource counts | implemented; partial parity |
| authenticated `/api/labels/` and `/api/labels/{id}/` GET | membership, resource-type filters/usage count | implemented; partial parity |
| authenticated `/api/properties/` and `/api/properties/{id}/` GET | membership, archived filters/config normalization/usage count | implemented; partial parity |
| authenticated `/api/issue-statuses/` GET | membership, idempotent status seeding, category/ordering | implemented; partial parity, mutates on read |
| authenticated `/api/tokens/` GET | human JWT/PAT; user ID from auth; active and expired but not revoked tokens, no secret/hash fields | implemented, DB-tested partial parity |
| authenticated `/api/invitations` and `/api/invitations/{id}` GET | user-scoped invitee email or ID, pending/expiry filter for list; 403 for another invitee in detail | implemented, DB-tested partial parity |
| other user/workspace/issue/agent/skill/chat/task GET routes | route-specific membership/roles, privacy, pagination and DB queries | unimplemented |

The slice reads real migrated PostgreSQL. No Go proxy, fixture response, or
501 route is counted. Auth accepts bearer `mul_` PATs (hash, revocation, expiry,
non-shadow user) and HS256 bearer/session JWTs, checks Go token/session-bound
CSRF on unsafe cookie requests and rejects task, daemon and cloud prefixes
rather than widening human privileges. The actual daemon, task and cloud
credential verifiers are NOT ported. It checks the staged emergency disabled
users. Header-supplied identity/actor/workspace IDs are ignored. URL gates
check membership in the SELECT. JSON fields, Go RFC3339-second timestamps,
error object, newline and Content-Length are matched for these routes.

Known differences: Go PAT cache can serve a revoked token until invalidation
or TTL; Rust always checks DB and performs a synchronous, best-effort
`last_used_at` UPDATE on every successful PAT read, not Go's asynchronous
cache-miss-only write. A failed usage UPDATE no longer blocks authentication.
Safe-method JWT browser-cookie renewal (half-TTL) and both CSRF cookies are ported for deployments without CloudFront; CDN cookie signing is not ported, and startup refuses a configured CloudFront key-pair ID. Avatar URL signing/storage resolution is not ported;
raw `avatar_url` is returned as with Go's nil-storage setting, so private
storage configurations must not use this service. Not production-ready.
No pagination is implemented for these eleven GETs; pagination parity
for other GETs remains entirely untested.

Synthetic integration on a run-owned PostgreSQL 16 container
(`multica_zbsi63` on 127.0.0.1:55463): both migration streams applied
(619 upstream + 44 ZBS), then `tests/api.rs` passed (owner/member/admin,
outsider and cross-workspace, empty list, revoked/expired/invalid PAT,
shadow/missing/expired JWT, missing and valid CSRF, actor-header spoofing,
404/400, no mutation, PAT last-used). Container cleaned by EXIT trap.
Auth DB unavailability returned 503 and a post-auth missing-table handler
returned 500 without leaking rows; the one-connection test pool also guards
against holding the auth connection while the handler acquires its own.
Shape comparisons against a running Go oracle have not been performed.

## Catalog follow-up (2026-10-31)

On a fresh task-owned PostgreSQL 16 database (`multica_zbsi63`, loopback
55464), both ledgers migrated (619 upstream + 44 ZBS) and the two ignored
`tests/api.rs` DB tests passed. A regression first reproduced a catalog
middleware deadlock with a one-connection pool, then verified the fix. Another
regression verified that a failed PAT usage write does not reject a valid PAT.
Default tests: 18 passed, 15 ignored; fmt, Clippy with `-D warnings` and
release build passed. Frozen staged Go source was rebuilt as a **local** oracle;
22 authenticated synthetic requests matched Go and Rust status/JSON (catalog,
filters, tenant denial, and status seeding) on a separate migrated parity DB.
See `catalog-parity-20261031.json`. This is neither the deployed executable
nor a speed benchmark; WebSocket/chat, task/cloud auth, storage avatar URLs,
CloudFront cookie signing, full renewal parity under production config, writes and most routes have no parity evidence.

## Cookie renewal follow-up (2026-10-31)

`api.rs` re-signs safe-method JWT cookie sessions within half the configured
TTL, retains/mints `sid` and issues auth plus legacy/session-bound CSRF cookies.
Bearer auth, invalid/expired sessions and unsafe methods do not renew. Runtime
`AUTH_TOKEN_TTL`, `COOKIE_DOMAIN` and `FRONTEND_ORIGIN` drive cookie lifetime,
domain and Secure flag. Unlike Go, malformed/unsupported nonempty TTL now
**fails startup** instead of silently defaulting; a configured
`CLOUDFRONT_KEY_PAIR_ID` also fails startup because Rust has no corresponding
CDN cookie signer. This is safer than silently renewing without CDN policy,
but is NOT configuration parity. Offline unit tests and the two DB-backed
API tests passed on a freshly migrated disposable PostgreSQL 16 after this
change; Clippy `-D warnings` passed. The earlier 22 Go/Rust JSON comparisons
pre-date this renewal change and do not verify Set-Cookie headers.

## Account-level follow-up (2026-11-01)

Three new GET registrations use frozen staged Go SQL contracts (`personal_access_token.sql`,
`invitation.sql`) with no workspace selector for account-level reads. PAT list excludes revoked
tokens but includes expired ones, never exposes hashes; invitation list joins workspace
and inviter, while detail matches Go's 400/404/403 and permits a user-ID-bound invite
whose email differs. No writes implemented. A new ignored DB test passes against a
run-owned PostgreSQL 16 container after 619+44 migrations, including auth-required,
other-user isolation, empty/expired lists, revoked PAT, failure responses and
read-only side effects. The two preceding API DB tests also passed on that instance;
container removed by EXIT trap. Default tests, Clippy and release build passed.
See `account-read-20261101.json` for run details and boundaries. There is still
no Go-vs-Rust differential run for these three routes or parity claim for
CloudFront, private avatars, machine credentials, writes, WS or benchmark.
