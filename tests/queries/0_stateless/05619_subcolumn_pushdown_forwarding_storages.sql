-- A subcolumn must not be pushed into a subquery that reads a forwarding storage (`Merge`, `Buffer`,
-- `MaterializedView`): the flattened name can resolve to a different column of the underlying table,
-- and the keys of the underlying table are not visible in the metadata of the forwarding storage.

DROP TABLE IF EXISTS t_fwd_merge;
DROP TABLE IF EXISTS t_fwd_buffer;
DROP TABLE IF EXISTS t_fwd_mv;
DROP TABLE IF EXISTS t_fwd_src;
DROP TABLE IF EXISTS t_fwd_null;
DROP TABLE IF EXISTS t_fwd_idx_merge;
DROP TABLE IF EXISTS t_fwd_idx;

CREATE TABLE t_fwd_src (tup Tuple(a UInt64, b String), `tup.a` UInt64) ENGINE = Memory;
INSERT INTO t_fwd_src VALUES ((42, 'x'), 99);

CREATE TABLE t_fwd_null (tup Tuple(a UInt64, b String)) ENGINE = Null;
CREATE TABLE t_fwd_merge (tup Tuple(a UInt64, b String)) ENGINE = Merge(currentDatabase(), '^t_fwd_src$');
CREATE TABLE t_fwd_buffer (tup Tuple(a UInt64, b String)) ENGINE = Buffer(currentDatabase(), t_fwd_src, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
CREATE MATERIALIZED VIEW t_fwd_mv TO t_fwd_src (tup Tuple(a UInt64, b String)) AS SELECT tup FROM t_fwd_null;

SELECT 'Merge';
SELECT tup.a FROM (SELECT tup FROM t_fwd_merge) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT tup.a FROM (SELECT tup FROM t_fwd_merge) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'Buffer';
SELECT tup.a FROM (SELECT tup FROM t_fwd_buffer) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT tup.a FROM (SELECT tup FROM t_fwd_buffer) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'MaterializedView';
SELECT tup.a FROM (SELECT tup FROM t_fwd_mv) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT tup.a FROM (SELECT tup FROM t_fwd_mv) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'Merge over an indexed table';
CREATE TABLE t_fwd_idx (tup Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY getSubcolumn(tup, 'a')
SETTINGS index_granularity = 128;
INSERT INTO t_fwd_idx SELECT (number, repeat('x', 10)) FROM numbers(65536);
CREATE TABLE t_fwd_idx_merge (tup Tuple(a UInt64, b String)) ENGINE = Merge(currentDatabase(), '^t_fwd_idx$');

SELECT count() FROM (SELECT tup FROM t_fwd_idx_merge) WHERE tup.a = 125
SETTINGS optimize_push_subcolumns_into_subqueries = 0, force_primary_key = 1, max_rows_to_read = 128;
SELECT count() FROM (SELECT tup FROM t_fwd_idx_merge) WHERE tup.a = 125
SETTINGS optimize_push_subcolumns_into_subqueries = 1, force_primary_key = 1, max_rows_to_read = 128;

DROP TABLE t_fwd_mv;
DROP TABLE t_fwd_buffer;
DROP TABLE t_fwd_merge;
DROP TABLE t_fwd_src;
DROP TABLE t_fwd_null;
DROP TABLE t_fwd_idx_merge;
DROP TABLE t_fwd_idx;
