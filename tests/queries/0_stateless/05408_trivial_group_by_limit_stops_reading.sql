-- Without aggregate functions, `GROUP BY ... LIMIT n` stops reading once enough keys are found
-- (https://github.com/ClickHouse/ClickHouse/issues/8777); otherwise the infinite sources hit `max_rows_to_read`.

SET optimize_trivial_group_by_limit_query = 1, exact_rows_before_limit = 0;
SET max_rows_to_read = 100000000, read_overflow_mode = 'throw';

SELECT number FROM system.numbers GROUP BY number LIMIT 5
SETTINGS max_threads = 1, log_comment = 'gbl_stop_single_thread' FORMAT Null;

-- The second stream never reaches the cap by itself.
SELECT k FROM (SELECT number AS k FROM system.numbers UNION ALL SELECT 1 AS k FROM system.numbers) GROUP BY k LIMIT 5
SETTINGS max_threads = 2, log_comment = 'gbl_stop_stream_with_single_key' FORMAT Null;

-- The second stream filters out all its rows, so its aggregation never receives a chunk.
SELECT k FROM (SELECT number AS k FROM system.numbers UNION ALL SELECT number AS k FROM system.numbers WHERE cityHash64(number) = 0)
GROUP BY k LIMIT 5
SETTINGS max_threads = 2, log_comment = 'gbl_stop_stream_without_rows' FORMAT Null;

-- The same when the first stream ends with the chunk that has enough keys.
SELECT k FROM (SELECT number AS k FROM numbers(1000) UNION ALL SELECT number AS k FROM system.numbers WHERE cityHash64(number) = 0)
GROUP BY k LIMIT 5
SETTINGS max_threads = 2, max_block_size = 1000, log_comment = 'gbl_stop_stream_ends_with_enough_keys' FORMAT Null;

-- Exactly `LIMIT` distinct keys.
SELECT number % 5 FROM system.numbers GROUP BY number % 5 LIMIT 5
SETTINGS max_threads = 1, log_comment = 'gbl_stop_exact_key_count' FORMAT Null;

-- The same with external aggregation, which empties the table on every block.
SELECT cityHash64(number % 5) AS k FROM system.numbers GROUP BY k LIMIT 5
SETTINGS max_threads = 1, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1,
    log_comment = 'gbl_stop_exact_key_count_external' FORMAT Null;

SELECT number FROM remote('127.0.0.{1,2}', system.numbers) GROUP BY number LIMIT 5
SETTINGS log_comment = 'gbl_stop_distributed' FORMAT Null;

SELECT k FROM remote('127.0.0.2', view(SELECT number AS k FROM system.numbers UNION ALL SELECT 1 AS k FROM system.numbers))
GROUP BY k LIMIT 5
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, max_threads = 2, log_comment = 'gbl_stop_serialized_plan' FORMAT Null;

-- User-set `any` mode without `LIMIT`.
SELECT number % 1000 FROM system.numbers GROUP BY number % 1000
SETTINGS max_threads = 1, max_block_size = 65536, max_rows_to_group_by = 10, group_by_overflow_mode = 'any',
    log_comment = 'gbl_stop_user_overflow_mode_any' FORMAT Null;

DROP TABLE IF EXISTS t_group_by_limit_stops_reading;
CREATE TABLE t_group_by_limit_stops_reading (k UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_group_by_limit_stops_reading SELECT number FROM numbers(2000000);

SELECT k FROM t_group_by_limit_stops_reading GROUP BY k LIMIT 5
SETTINGS max_threads = 4, log_comment = 'gbl_stop_merge_tree' FORMAT Null;

SELECT k, count() FROM t_group_by_limit_stops_reading GROUP BY k LIMIT 5
SETTINGS max_threads = 4, log_comment = 'gbl_stop_merge_tree_aggregate' FORMAT Null;

-- Aggregation in order of the sorting key, whose only value makes a single run of the whole table.
DROP TABLE IF EXISTS t_group_by_limit_stops_reading_in_order;
CREATE TABLE t_group_by_limit_stops_reading_in_order (p UInt8, k UInt64) ENGINE = MergeTree ORDER BY p
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_group_by_limit_stops_reading_in_order SELECT 0, number % 1000 FROM numbers(2000000);

SELECT p, k FROM t_group_by_limit_stops_reading_in_order GROUP BY p, k LIMIT 5
SETTINGS max_threads = 1, optimize_aggregation_in_order = 1, log_comment = 'gbl_stop_merge_tree_in_order' FORMAT Null;
SELECT p, k FROM t_group_by_limit_stops_reading_in_order GROUP BY p, k LIMIT 5
SETTINGS max_threads = 4, optimize_aggregation_in_order = 1, log_comment = 'gbl_stop_merge_tree_in_order_threads' FORMAT Null;

-- The part with the aggregate projection has enough keys; the reading of the part without it has to stop too.
DROP TABLE IF EXISTS t_group_by_limit_stops_reading_projection;
CREATE TABLE t_group_by_limit_stops_reading_projection (k UInt64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_group_by_limit_stops_reading_projection;
INSERT INTO t_group_by_limit_stops_reading_projection SELECT 7 FROM numbers(5000000);
ALTER TABLE t_group_by_limit_stops_reading_projection ADD PROJECTION p (SELECT k, count() GROUP BY k);
INSERT INTO t_group_by_limit_stops_reading_projection SELECT number FROM numbers(1000);

SELECT k FROM t_group_by_limit_stops_reading_projection GROUP BY k LIMIT 5
SETTINGS max_threads = 4, max_streams_for_merge_tree_reading = 1, optimize_use_projections = 1, force_optimize_projection = 1,
    log_comment = 'gbl_stop_projection' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, result_rows,
    multiIf(startsWith(log_comment, 'gbl_stop_merge_tree'), toString(read_rows < 2000000), log_comment = 'gbl_stop_projection', toString(read_rows < 5000000), '-')
FROM system.query_log
WHERE current_database = currentDatabase() AND startsWith(log_comment, 'gbl_stop_') AND type = 'QueryFinish' AND is_initial_query
ORDER BY log_comment;

DROP TABLE t_group_by_limit_stops_reading;
DROP TABLE t_group_by_limit_stops_reading_projection;
DROP TABLE t_group_by_limit_stops_reading_in_order;
