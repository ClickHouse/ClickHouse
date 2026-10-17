-- Without aggregate functions, `GROUP BY ... LIMIT n` stops reading once enough keys are found
-- (https://github.com/ClickHouse/ClickHouse/issues/8777); otherwise the infinite sources hit `max_rows_to_read`.

SET optimize_trivial_group_by_limit_query = 1, exact_rows_before_limit = 0;
SET max_rows_to_read = 100000000, read_overflow_mode = 'throw';

SELECT number FROM system.numbers GROUP BY number LIMIT 5
SETTINGS max_threads = 1, log_comment = 'gbl_stop_single_thread' FORMAT Null;

-- The second stream never reaches the cap by itself.
SELECT k FROM (SELECT number AS k FROM system.numbers UNION ALL SELECT 1 AS k FROM system.numbers) GROUP BY k LIMIT 5
SETTINGS max_threads = 2, log_comment = 'gbl_stop_stream_with_single_key' FORMAT Null;

SELECT number FROM remote('127.0.0.{1,2}', system.numbers) GROUP BY number LIMIT 5
SETTINGS log_comment = 'gbl_stop_distributed' FORMAT Null;

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

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, result_rows, if(startsWith(log_comment, 'gbl_stop_merge_tree'), toString(read_rows < 2000000), '-')
FROM system.query_log
WHERE current_database = currentDatabase() AND startsWith(log_comment, 'gbl_stop_') AND type = 'QueryFinish' AND is_initial_query
ORDER BY log_comment;

DROP TABLE t_group_by_limit_stops_reading;
