# Query route registration audit (ZBSI-69)

Static registration inventory for the 76 query routes frozen in
`baseline/sprint-route-ownership-20261102.json` (`sprint_owner == "queries"`).

```sh
python3 queries_sprint_route_audit.py \
  --go-root ../server \
  --rust-root . \
  --matrix baseline/sprint-route-ownership-20261102.json \
  --output queries_sprint_route_audit.json
```

The scanner resolves the canonical `cmd/server/router.go` the way it is
actually written: chi subtree prefixes come from nested `r.Route(...)` blocks,
`r.Group(...)` blocks contribute no prefix, and `r.With(...).Post(...)` forms
are read as registrations of the enclosing router.

Frozen assertions — the audit fails loudly instead of degrading to an empty
pass:

- exactly one `chi.NewRouter()` and one `NewRouterWithOptions` in the file, with
  every `r.Route` block inside that function;
- every matrix query row matches exactly one parsed registration on
  `(method, handler, leaf path literal)`, and no two rows share a registration;
- the inventory is exactly 76 rows with 76 distinct `(method, path)` pairs and
  fully literal path and prefix segments.

Exit state on the 2026-11-18 canonical tree: 76/76 resolved, 0 absent, 484
registrations parsed by both the scanner and a `grep -c` cross-check.

**This is a registration inventory, not parity.** It says nothing about
response bodies, auth/middleware reachability, events, or production
registration. Every row is `present_not_parity_proven`; HTTP-level evidence for
these routes lives in `tests/queries_sprint_*.rs` and the differential run, not
here. The `rust_registration_candidates` field is a static `(method, normalized
path)` lookup, and 19 rows list two candidates because this tree registers both
`x` and `x/` slash twins.