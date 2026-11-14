-- Pushing a subcolumn of a key column into a subquery must not break index analysis.

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_skip;

CREATE TABLE t_pk (tup Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY getSubcolumn(tup, 'a')
SETTINGS index_granularity = 128;
INSERT INTO t_pk SELECT (number, repeat('x', 10)) FROM numbers(65536);

SELECT count() FROM (SELECT tup FROM t_pk) WHERE tup.a = 125
SETTINGS optimize_push_subcolumns_into_subqueries = 0, force_primary_key = 1, max_rows_to_read = 128;
SELECT count() FROM (SELECT tup FROM t_pk) WHERE tup.a = 125
SETTINGS optimize_push_subcolumns_into_subqueries = 1, force_primary_key = 1, max_rows_to_read = 128;

CREATE TABLE t_skip (id UInt64, tup Tuple(a UInt64, b String), INDEX idx getSubcolumn(tup, 'a') TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128;
INSERT INTO t_skip SELECT number, (number, repeat('x', 10)) FROM numbers(65536);

SELECT count() FROM (SELECT tup FROM t_skip) WHERE tup.a = 125
SETTINGS optimize_push_subcolumns_into_subqueries = 0, force_data_skipping_indices = 'idx', max_rows_to_read = 128;
SELECT count() FROM (SELECT tup FROM t_skip) WHERE tup.a = 125
SETTINGS optimize_push_subcolumns_into_subqueries = 1, force_data_skipping_indices = 'idx', max_rows_to_read = 128;

DROP TABLE t_pk;
DROP TABLE t_skip;
