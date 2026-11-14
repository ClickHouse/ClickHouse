-- The synthetic table used to analyze a standalone expression must not expose its own
-- `_table` and `_database` virtual columns as inputs of the expression.

DROP TABLE IF EXISTS t_dummy_virtuals;

CREATE TABLE t_dummy_virtuals (x UInt64, INDEX idx length(_table) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x; -- { serverError UNKNOWN_IDENTIFIER }
CREATE TABLE t_dummy_virtuals (x UInt64, INDEX idx length(_database) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x; -- { serverError UNKNOWN_IDENTIFIER }

CREATE TABLE t_dummy_virtuals (x UInt64, t Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_dummy_virtuals ADD INDEX idx length(_table) TYPE minmax GRANULARITY 1; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE t_dummy_virtuals ADD INDEX idx length(_database) TYPE minmax GRANULARITY 1; -- { serverError UNKNOWN_IDENTIFIER }

-- Subcolumns are still resolved.
ALTER TABLE t_dummy_virtuals ADD INDEX idx length(t.b) + t.a TYPE minmax GRANULARITY 1;
INSERT INTO t_dummy_virtuals VALUES (1, (2, 'abc'));
SELECT x, t.a, t.b FROM t_dummy_virtuals WHERE length(t.b) + t.a = 5;

DROP TABLE t_dummy_virtuals;
