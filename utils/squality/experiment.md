# SQuaLity experiment on 2026-10-05

SQuaLity `a471c3f0e0f7faa0faad8d103da9043af6ce57e1` parsed SQLite SQLLogicTest `c67f97bf3ca7e590d12e073408bcacaf2ff0f3a0` and DuckDB `96df096fb00184f615770231fcb61884c97d77eb`. The profile comparison used ClickHouse master `392f555ff7db8217ad5b8929560575041e97823e` (ARM, version `26.10.1.1602`), the newest completed master build when the run began. The SQLite and DuckDB samples were rerun on newer master `2cf1c54ffcb2d553aaed4650e9c371f8d62356ba` (version `26.10.1.1607`). Tests ran on the `dev` VM in dedicated servers. Raw JSONL and logs are at `/home/ubuntu/clickhouse/tmp/squality/results` and `/home/ubuntu/clickhouse/tmp/squality/build` there.

| Suite and bound | Query pass | Result mismatch | Main limit |
| --- | ---: | ---: | --- |
| SQLite `select*.test`, five files, first 1,000 records each, default profile (`26.10.1.1602`) | 915 | 1,992 | One file stops at `PRIMARY KEY` with `Memory` |
| Same SQLite files, SQL profile (`26.10.1.1602` and `26.10.1.1607`) | 2,007 | 900 | Same `PRIMARY KEY` stop |
| SQLite random `select`, ten seeded files, first 100 records each | 656 | 21 | Nine file limits, 117 source condition skips |
| DuckDB SQL, 750 seeded files, first 50 records each (`26.10.1.1607`) | 279 | 21 | 315 files need unsupported harness features; 291 setups blocked |

The combined SQL profile converted 1,095 SQLite mismatches into passes and introduced three mismatches, a net improvement of 1,092 passes on the identical 2,907 executed query records. All 900 remaining mismatches in this sample agree with the SQLite reference result, so they are real cross-engine differences rather than bad expected hashes. The 21 random SQLite mismatches all involve integer `/`: SQLite divides integers before later arithmetic, while ClickHouse `/` produces a floating result. There is no session setting for SQLite-style integer division.

The DuckDB sample also has 1,080 SQL exceptions, 261 expected errors, 18 statements expected to fail that succeeded, 357 passed statements, 39 skipped DuckDB settings, 22 excluded plan checks, 23 missing-fixture exclusions, and two blocked database-admin tests. Most exceptions are syntax, unsupported functions, or engine-specific DDL. DuckDB plan expectations and data fixtures are not portable. The prior master returned 278 passes and 22 mismatches. The one changed result is the row order of `SELECT 1 UNION ALL SELECT 'asdf'` without `ORDER BY`; it is not evidence of a correctness fix. An earlier trial let `DETACH DATABASE system` from DuckDB's suite affect the dedicated server; the adapter now blocks database-admin commands, and the reported sample ran on a fresh protected server.

One DuckDB case gives a concise, reproducible `GROUPING` discrepancy with no setup tables:

```sql
SELECT grouping(1 + 1, 2 + 0), count(*)
FROM (VALUES (1), (2)) AS t(x)
GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ())
ORDER BY 1;
```

ClickHouse masters `392f555ff7db8217ad5b8929560575041e97823e` and `2cf1c54ffcb2d553aaed4650e9c371f8d62356ba` return `(0, 2), (0, 2), (3, 2)`; DuckDB and its source test return `(1, 2), (2, 2), (3, 2)`. Replacing the two constant expressions with distinct column expressions `x + 1` and `x + 2 - 1` produces the expected masks in both engines. This points to constant folding merging two syntactically distinct grouping keys before `GROUPING` calculates its bits. [ClickHouse issue #117904](https://github.com/ClickHouse/ClickHouse/issues/117904) concerns lost duplicate keys in `CUBE` and `ROLLUP`; this explicit `GROUPING SETS` mask case is related but distinct. It is a credible correctness finding pending maintainer triage.

Two other differences are already explained by ClickHouse semantics. With an unaliased outer table `t1`, a subquery `FROM t1 AS x WHERE x.b < t1.b` binds `t1.b` to its own input in ClickHouse, returning zero counts; explicitly aliasing the outer table as `y` and using `y.b` returns the expected correlated result. `CASE NULL WHEN NULL THEN 444 ELSE 555 END` returns `444` in ClickHouse and `555` in SQLite. ClickHouse [test `04259_case_with_expression_null_equals_null_101262.sql`](../../tests/queries/0_stateless/04259_case_with_expression_null_equals_null_101262.sql) explicitly preserves this latter behavior, so it is a dialect difference rather than an unrecognized regression.

The DuckDB added-column case shows that `Memory` returns old rows with `l=0` after `ALTER TABLE test ADD COLUMN l INTEGER DEFAULT 3`; `MergeTree` returns `l=3`, matching DuckDB. [ClickHouse issue #69331](https://github.com/ClickHouse/ClickHouse/issues/69331) describes a related longstanding difference in `Memory` default handling. Using `MergeTree` addresses that case without a ClickHouse code change. For index tests, `MergeTree`, `data_type_default_nullable=0`, and `allow_create_index_without_type=1` allow ordinary setup, but `UNIQUE` indexes block the sampled files before any query checks.

The adapter can detect wrong results and isolate a useful `GROUPING` candidate, but the DuckDB suite currently has a low portable-query yield. The SQLite randomized sample is a cleaner source of additional result checks once integer-division cases are classified as a known dialect difference. No ClickHouse engine changes were made.
