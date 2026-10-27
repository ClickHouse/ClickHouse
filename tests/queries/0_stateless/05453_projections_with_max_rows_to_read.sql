-- A throwing `max_rows_to_read` over the rows of the whole table still lets projections cut the read under it.

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0;
SET use_query_condition_cache = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_projection_index;
CREATE TABLE t_projection_index (key UInt64, tok UInt64, payload String, PROJECTION by_tok (SELECT _part_offset ORDER BY tok))
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 100, index_granularity_bytes = 0;

-- 100000 rows. `tok = 84` matches 100 rows, one in each of 100 granules: the projection index keeps 10000 rows.
INSERT INTO t_projection_index SELECT number, number % 1000 * 2, 'x' FROM numbers(100000);

SELECT countIf(explain LIKE '%Name: by_tok%') FROM (EXPLAIN projections = 1 SELECT count(), any(payload) FROM t_projection_index WHERE tok = 84 SETTINGS max_rows_to_read = 50000);
SELECT count(), any(payload) FROM t_projection_index WHERE tok = 84 SETTINGS max_rows_to_read = 50000;
-- absent but inside the range of the projection key, then outside of it
SELECT count(), any(payload) FROM t_projection_index WHERE tok = 85 SETTINGS max_rows_to_read = 50000;
SELECT count(), any(payload) FROM t_projection_index WHERE tok = 1000000 SETTINGS max_rows_to_read = 50000;
SELECT count(), any(payload) FROM t_projection_index WHERE tok = 84 SETTINGS max_rows_to_read = 5000; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_projection_index;

-- A normal projection present in only some parts: the read of the other parts used to keep every part, and the
-- projection was applied to it again and again.
DROP TABLE IF EXISTS t_partial_projection;
CREATE TABLE t_partial_projection (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 100, index_granularity_bytes = 0;
SYSTEM STOP MERGES t_partial_projection;

INSERT INTO t_partial_projection SELECT number, number % 1000 FROM numbers(10000);
ALTER TABLE t_partial_projection ADD PROJECTION by_v (SELECT * ORDER BY v);
INSERT INTO t_partial_projection SELECT number + 10000, number % 1000 FROM numbers(10000);

SELECT count() FROM t_partial_projection WHERE v = 7 SETTINGS max_rows_to_read = 15000;

DROP TABLE t_partial_projection;
