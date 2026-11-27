-- Tags: no-parallel-replicas
-- no-parallel-replicas: streaming reads are not supported over parallel replicas.

-- `query_cache_use_only_when_data_was_not_changed` must not cache the result of a `STREAM` read. A bounded
-- stream reads only the safe streaming prefix, which can stop before a committed part (behind the block
-- number of an unfinished insert) and grow later without any change of the active parts that the
-- modification hash covers. The same read without `STREAM` is cached: each query runs twice, and only the
-- second run of the plain read is a cache hit. `REFRESH ... IF CHANGED APPEND INCREMENTAL` injects such a
-- read, and fails closed in the same way.

SET enable_streaming_queries = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_stream_cache;

CREATE TABLE t_stream_cache (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_stream_cache SELECT number, number * 10 FROM numbers(5);

SET use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

SELECT 'stream', count(), sum(v) FROM t_stream_cache STREAM BOUNDED;
SELECT 'stream', count(), sum(v) FROM t_stream_cache STREAM BOUNDED;
SELECT 'plain', count(), sum(v) FROM t_stream_cache;
SELECT 'plain', count(), sum(v) FROM t_stream_cache;

SET use_query_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT extract(query, '^SELECT \'([a-z]+)\''), ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND (query LIKE 'SELECT \'stream\'%' OR query LIKE 'SELECT \'plain\'%')
ORDER BY event_time_microseconds;

DROP TABLE t_stream_cache;
