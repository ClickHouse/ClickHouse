-- When the throw threshold is not above the delay threshold, an insert past the delay threshold gets the maximal delay.

DROP TABLE IF EXISTS t_delay_table_level;
DROP TABLE IF EXISTS t_delay_query_level;

CREATE TABLE t_delay_table_level (x UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS parts_to_delay_insert = 3, parts_to_throw_insert = 2, max_delay_to_insert = 1, min_delay_to_insert_ms = 7,
    dead_blobs_to_delay_insert = 0, dead_blobs_to_throw_insert = 0;

CREATE TABLE t_delay_query_level (x UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS parts_to_delay_insert = 3, max_delay_to_insert = 1, min_delay_to_insert_ms = 7,
    dead_blobs_to_delay_insert = 0, dead_blobs_to_throw_insert = 0;

SYSTEM STOP MERGES t_delay_table_level;
SYSTEM STOP MERGES t_delay_query_level;

-- One part per block; the sink rejects nothing after it started, it only delays the later blocks.
INSERT INTO t_delay_table_level SELECT number FROM numbers(6)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1, async_insert = 0;
INSERT INTO t_delay_query_level SELECT number FROM numbers(6)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1, async_insert = 0, parts_to_throw_insert = 2;

SYSTEM FLUSH LOGS query_log;

-- Milliseconds per delayed block: `max_delay_to_insert * 1000`, never `min_delay_to_insert_ms`.
SELECT ProfileEvents['DelayedInsertsMilliseconds'] / ProfileEvents['DelayedInserts']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND event_date >= yesterday() AND query LIKE '%INSERT INTO t_delay_%'
ORDER BY event_time_microseconds;

SELECT count() FROM t_delay_table_level;
SELECT count() FROM t_delay_query_level;

DROP TABLE t_delay_table_level;
DROP TABLE t_delay_query_level;
