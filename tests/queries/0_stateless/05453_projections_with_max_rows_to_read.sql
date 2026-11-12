-- A throwing `max_rows_to_read` over the rows of the whole table: projections are analyzed against the read itself.

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0;
SET use_query_condition_cache = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_projection_index;
CREATE TABLE t_projection_index (key UInt64, tok UInt64, aux UInt64, payload String) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_projection_index;

-- 1000 rows without the projection, then 100000 rows with it.
INSERT INTO t_projection_index SELECT number, number % 1000 * 2, number % 3, 'x' FROM numbers(1000);
ALTER TABLE t_projection_index ADD PROJECTION by_tok (SELECT aux, _part_offset ORDER BY tok);
INSERT INTO t_projection_index SELECT number, number % 1000 * 2, number % 3, 'x' FROM numbers(100000);

SELECT countIf(explain LIKE '%Name: by_tok%') FROM (EXPLAIN projections = 1 SELECT count(), any(payload) FROM t_projection_index WHERE tok = 84 SETTINGS max_rows_to_read = 50000);

DROP TABLE t_projection_index;

-- A normal projection present in only some parts: the read of the other parts used to keep every part, and the
-- projection was applied to it again and again.
DROP TABLE IF EXISTS t_partial_projection;
CREATE TABLE t_partial_projection (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_partial_projection;

INSERT INTO t_partial_projection SELECT number, number % 1000 FROM numbers(10000);
ALTER TABLE t_partial_projection ADD PROJECTION by_v (SELECT * ORDER BY v);
INSERT INTO t_partial_projection SELECT number + 10000, number % 1000 FROM numbers(10000);

SELECT count() FROM t_partial_projection WHERE v = 7 SETTINGS max_rows_to_read = 15000;

DROP TABLE t_partial_projection;

-- A projection with finer granules than the table can read more marks than the over-limit table read and still stay
-- under the limit.
DROP TABLE IF EXISTS t_fine_projection;
CREATE TABLE t_fine_projection (key UInt64, tok UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
ALTER TABLE t_fine_projection ADD PROJECTION by_tok (SELECT * ORDER BY tok) WITH SETTINGS (index_granularity = 10);
INSERT INTO t_fine_projection SELECT number, number % 200 FROM numbers(100000);

SELECT count() FROM t_fine_projection WHERE key < 30000 AND tok = 5 SETTINGS max_rows_to_read = 10000, force_optimize_projection = 0, prefer_optimize_projection = 0;

DROP TABLE t_fine_projection;

-- The projection drops a whole part from the table read, which brings it under the limit: it is compared by marks again.
DROP TABLE IF EXISTS t_pruned_projection;
CREATE TABLE t_pruned_projection (key UInt64, tok UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_pruned_projection;

INSERT INTO t_pruned_projection SELECT number, number % 200 FROM numbers(4000);
ALTER TABLE t_pruned_projection ADD PROJECTION by_tok (SELECT * ORDER BY tok) WITH SETTINGS (index_granularity = 10);
INSERT INTO t_pruned_projection SELECT 100000 + number, 999 FROM numbers(20000);
INSERT INTO t_pruned_projection SELECT 200000 + number, 5 FROM numbers(4000);

SELECT count() FROM t_pruned_projection WHERE key < 201000 AND tok = 5 SETTINGS max_rows_to_read = 7000, force_optimize_projection = 0, prefer_optimize_projection = 0;

DROP TABLE t_pruned_projection;
