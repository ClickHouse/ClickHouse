-- The filter left in WHERE after a partial move to PREWHERE must keep its query condition cache key,
-- so that a repeated query skips the granules the whole condition rules out.
-- https://github.com/ClickHouse/ClickHouse/issues/128229

-- add_minmax_index_for_numeric_columns=0: Would use the index instead (used before the query condition cache)
-- w/o local plan for parallel replicas the filter steps would be executed as part of remote queries
SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1;
SET move_all_conditions_to_prewhere = 0;
-- The filter writes into the query condition cache only the chunks it empties as a whole, so make a chunk one granule.
SET max_block_size = 128;

DROP TABLE IF EXISTS t;
CREATE TABLE t (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t SELECT number, intDiv(number, 2000) FROM numbers(200000);

-- `intDiv(key, 128) % 100 = 0` is moved to PREWHERE (16 granules), `v < 50` stays in WHERE (8 granules for the whole condition).
SELECT count() FROM t WHERE v < 50 AND intDiv(key, 128) % 100 = 0 SETTINGS log_comment = '05487_1' FORMAT Null;
SELECT count() FROM t WHERE v < 50 AND intDiv(key, 128) % 100 = 0 SETTINGS log_comment = '05487_2' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows, ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment IN ('05487_1', '05487_2')
ORDER BY event_time_microseconds;

DROP TABLE t;
