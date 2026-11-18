# Recent-comments cost probe (ZBSI-69)

```sh
python3 sprint-gate.py --issue ZBSI-69 --kind db --log bench-comments.log \
  --timeout-seconds 480 -- bash queries_sprint_comments_bench.sh
```

Result on 2026-11-18 (disposable PostgreSQL 16, 2 CPU / 768 MiB, real indexes
from `migrations/`), seeded exactly like
`tests/queries_sprint_comments.rs::comments_recent_over_twenty_thousand_is_complete_and_cursor_isolated`:
one root with 20,001 replies, one older thread, one malformed cross-tenant child.

| statistics | recent=1 | recent=50 |
| --- | --- | --- |
| as inserted, first execution | **40,237 ms** | 45 ms |
| as inserted, second execution | 50 ms | 46 ms |
| after `ANALYZE comment` | 45–50 ms | 45 ms |

The cold plan is nested-loop on both recursion levels — `Rows Removed by Join
Filter: 200,030,001` in the `Recursive Union` (17.2 s) and `200,050,003` in the
final join (40.2 s) — because with no statistics the planner estimates `rows=1`
at every step. The second execution and every analyzed execution use hash joins
and finish in ~45 ms.

So the cost is plan choice under missing statistics, not an inherent
`O(n²)` result for 20k comments: any measurement of this route must say whether
the table was analyzed. The previous 15.05 s figure is the same regime (nested
loop, 200M join-filter rows) on a different day and estimate.

The Go handler runs this identical SQL and sets no statement timeout, so it
takes the same path; that is why the port keeps the query byte-identical instead
of rewriting it. Making this cheap in the cold case needs a decision outside
this issue (analyze after bulk loads, or a SQL change on both sides).