SET query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10, query_plan_join_swap_table = false; -- the right side must stay on the right
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- disable automatic spilling
SET enable_parallel_replicas = 0; -- join optimization is disabled with parallel replicas
SET collect_hash_table_stats_during_joins = 1, max_entries_for_hash_table_stats = 10000;
SET join_algorithm = 'hash', hash_join_layout = 'auto', parallel_hash_join_threshold = 1000;

CREATE TABLE lhs (a UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE rhs (a UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO lhs SELECT number FROM numbers(1000);
-- Not empty: an empty table is read by `ReadNothingStep`, whose statistics key is shared by every empty table of the server
INSERT INTO rhs VALUES (0);

-- The right side has no row estimate and reads nothing: parallel layout on a cold run, serial once the empty build is cached
SELECT * FROM lhs AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_empty_cold' FORMAT Null;
SELECT * FROM lhs AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_empty_warm' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment IN ('05632_empty_cold', '05632_empty_warm')
ORDER BY event_time_microseconds;
