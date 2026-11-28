-- Async inserts with deduplication in dependent materialized views support only one view block per
-- source block. A view whose query returns its output in several chunks used to fail with
-- NOT_IMPLEMENTED once the output exceeded `min_insert_block_size_rows`, even when the view reduces
-- rows. The view output of one source block must now be squashed into one block.

DROP TABLE IF EXISTS mv_agg;
DROP TABLE IF EXISTS mv_arr;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst_agg;
DROP TABLE IF EXISTS dst_arr;

CREATE TABLE src (k UInt64, g UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS non_replicated_deduplication_window = 100;
CREATE TABLE dst_agg (g UInt64, c UInt64) ENGINE = MergeTree ORDER BY g
SETTINGS non_replicated_deduplication_window = 100;
CREATE TABLE dst_arr (k UInt64, x UInt8) ENGINE = MergeTree ORDER BY (k, x)
SETTINGS non_replicated_deduplication_window = 100;

SYSTEM STOP MERGES dst_agg;
SYSTEM STOP MERGES dst_arr;

-- Reduces rows: two source rows per group.
CREATE MATERIALIZED VIEW mv_agg TO dst_agg AS SELECT g, count() AS c FROM src GROUP BY g;
-- Adds rows: three view rows per source row.
CREATE MATERIALIZED VIEW mv_arr TO dst_arr AS SELECT k, arrayJoin([1, 2, 3]) AS x FROM src;

SET async_insert = 1, wait_for_async_insert = 1;
SET deduplicate_insert = 'enable', deduplicate_blocks_in_dependent_materialized_views = 1;
-- The view queries return one row per chunk, and the squashing would emit a block every 2 rows.
SET max_block_size = 1, min_insert_block_size_rows = 2, min_insert_block_size_bytes = 0;

INSERT INTO src VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3);

SELECT 'insert';
SELECT count() FROM src;
SELECT g, c FROM dst_agg ORDER BY g;
SELECT count() FROM dst_arr;
SELECT table, count() FROM system.parts
WHERE database = currentDatabase() AND table IN ('dst_agg', 'dst_arr') AND active
GROUP BY table ORDER BY table;

-- A retry is deduplicated in the source table and in both views.
INSERT INTO src VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3);

SELECT 'retry';
SELECT count() FROM src;
SELECT sum(c) FROM dst_agg;
SELECT count() FROM dst_arr;

-- A retry and a new insert in the same flush: only the rows of the new insert are added.
SET wait_for_async_insert = 0, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 10000000;

INSERT INTO src VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3);
INSERT INTO src VALUES (11, 4), (12, 4), (13, 5), (14, 5), (15, 6), (16, 6);

SELECT 'retry and new insert in one flush';
SELECT length(entries.query_id) FROM system.asynchronous_inserts
WHERE database = currentDatabase() AND table = 'src';

SYSTEM FLUSH ASYNC INSERT QUEUE src;

SELECT count() FROM src;
SELECT g, c FROM dst_agg ORDER BY g;
SELECT count() FROM dst_arr;

DROP TABLE mv_agg;
DROP TABLE mv_arr;
DROP TABLE src;
DROP TABLE dst_agg;
DROP TABLE dst_arr;
