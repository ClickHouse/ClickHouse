# query-analyzer

Runs `QueryAnalysisPass` (the analyzer entry point, `QueryAnalyzer`) on a
`SELECT` query in a loop and reports timing statistics. Intended for profiling
and optimizing query analysis without the noise of planning and execution.

## Building

The util is built only when utils are enabled:

```bash
cmake -DENABLE_UTILS=1 <build_dir>
ninja -C <build_dir> query-analyzer
```

The binary is `<build_dir>/utils/query-analyzer/query-analyzer`.

## Usage

```bash
# Analyze a query 1000 times, print timing stats
query-analyzer -n 1000 "SELECT 1"

# Setup statements: everything before the last ';'-separated statement is
# executed first (CREATE TABLE, INSERT, SET, ...); the last statement is analyzed
query-analyzer -n 1000 "
    CREATE TABLE t (key UInt64, value String) ENGINE = Memory;
    SELECT key, length(value) FROM t GROUP BY key, length(value)"

# Read from stdin
query-analyzer -n 1000 < query.sql

# Apply settings
query-analyzer --setting max_subquery_depth=200 -n 100 "SELECT 1"

# Inspect the resolved tree
query-analyzer --dump-tree "SELECT 1 + 1"
```

Each iteration clones the unresolved query tree and uses a fresh query
context, so iterations are independent (scalar subquery results are not
cached across iterations). Only the analysis pass itself is timed; parsing
and query tree building are excluded (tree building is reported once,
separately). Scalar subqueries are executed during analysis as in
production; pass `--only-analyze` to skip executing them.

## Profiling with perf

```bash
perf record -g -- query-analyzer -n 10000 "$(cat query.sql)"
perf report

# Or a flame graph:
perf script | stackcollapse-perf.pl | flamegraph.pl > analyzer.svg
```

Use a build with debug info (`RelWithDebInfo`) for meaningful stacks.

## Limitations

- Tables live in an in-memory `default` database created at startup; there is
  no persistent storage and no server configuration. The `system` database is
  attached, so queries against `system.one`, `system.numbers`, etc. work.
- The analyzed (last) statement must be a `SELECT` (including `UNION` /
  `INTERSECT` / `EXCEPT`).
- `INSERT ... VALUES` inline data extends to the end of the line (the same
  rule as `clickhouse-client` multiquery input), so a setup `INSERT` must be
  followed by a newline — not just a `;` — before the next statement.
- `SET` statements in the setup part are applied to the global context;
  settings can also be passed via `--setting key=value`.
