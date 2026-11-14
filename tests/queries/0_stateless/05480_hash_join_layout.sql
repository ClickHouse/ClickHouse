-- `hash_join_layout = 'serial'` and `'parallel'` force the layout of a hash join whatever the estimate. With `auto`, `parallel_hash_join_threshold` decides.

SET max_threads = 4;
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0, query_plan_join_shard_by_pk_ranges = 0, query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0, collect_hash_table_stats_during_joins = 0;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number FROM numbers(10);
INSERT INTO r SELECT number FROM numbers(10);

-- With `query_plan_optimize_join_order_limit = 0` the right side has no estimate. With `10` its estimate is its 10 rows, below the default `parallel_hash_join_threshold`.
SELECT * FROM l JOIN r ON l.k = r.k FORMAT Null
SETTINGS hash_join_layout = 'serial', query_plan_optimize_join_order_limit = 0, log_comment = '05480_serial_no_estimate';
SELECT * FROM l JOIN r ON l.k = r.k FORMAT Null
SETTINGS hash_join_layout = 'serial', query_plan_optimize_join_order_limit = 10, log_comment = '05480_serial_estimate';
SELECT * FROM l JOIN r ON l.k = r.k FORMAT Null
SETTINGS hash_join_layout = 'parallel', query_plan_optimize_join_order_limit = 0, log_comment = '05480_parallel_no_estimate';
SELECT * FROM l JOIN r ON l.k = r.k FORMAT Null
SETTINGS hash_join_layout = 'parallel', query_plan_optimize_join_order_limit = 10, log_comment = '05480_parallel_estimate';
SELECT * FROM l JOIN r ON l.k = r.k FORMAT Null
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = '05480_auto_no_estimate';
SELECT * FROM l JOIN r ON l.k = r.k FORMAT Null
SETTINGS query_plan_optimize_join_order_limit = 10, log_comment = '05480_auto_estimate';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND match(log_comment, '^05480_[a-z_]+$')
ORDER BY event_time_microseconds;

DROP TABLE l;
DROP TABLE r;
