-- `EXPLAIN WHATIF table = 1` gives one row with typed columns for each candidate

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t SELECT number, number % 100 FROM numbers(1000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
CREATE HYPOTHETICAL PROJECTION p_b ON t (SELECT a, b ORDER BY b);
-- the query reads `b`, which this projection does not have
CREATE HYPOTHETICAL PROJECTION p_a ON t (SELECT a ORDER BY a);

SELECT '-- the statement';
EXPLAIN WHATIF table = 1 SELECT a FROM t WHERE b = 42;

SELECT '-- the column types';
SELECT toTypeName(name), toTypeName(kind), toTypeName(status), toTypeName(marks), toTypeName(marks_low), toTypeName(marks_high),
    toTypeName(rows), toTypeName(verdict), toTypeName(source), toTypeName(reason)
FROM (EXPLAIN WHATIF table = 1 SELECT a FROM t WHERE b = 42) LIMIT 1;

SELECT '-- a query on the rows';
SELECT name, marks FROM (EXPLAIN WHATIF table = 1 SELECT a FROM t WHERE b = 42) WHERE status = 'applicable' ORDER BY marks, name;

SELECT '-- the text report stays the default';
SELECT count() > 0 FROM (EXPLAIN WHATIF SELECT a FROM t WHERE b = 42) WHERE explain LIKE 'Baseline%';

EXPLAIN WHATIF table = 2 SELECT a FROM t WHERE b = 42; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE t;
