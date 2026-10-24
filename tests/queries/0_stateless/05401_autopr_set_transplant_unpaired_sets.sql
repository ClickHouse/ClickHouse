-- When automatic parallel replicas switches a query, the sets the single-node plan has already
-- prepared are handed to the parallel-replicas plan, paired by hash. A set that has no counterpart
-- used to fail the query with "Cannot find a matching set in the map of sets from single-replica plan";
-- now the query keeps the single-node plan instead.
-- https://github.com/ClickHouse/ClickHouse/issues/124322

DROP TABLE IF EXISTS t_autopr_unpaired_sets;
DROP TABLE IF EXISTS r_autopr_unpaired_sets;

CREATE TABLE t_autopr_unpaired_sets (k String, v UInt64, g UInt8) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_autopr_unpaired_sets SELECT toString(number % 2000), number % 1000, number % 2 FROM numbers(1000000);

CREATE TABLE r_autopr_unpaired_sets (k String) ENGINE = MergeTree ORDER BY k;
INSERT INTO r_autopr_unpaired_sets SELECT toString(number * 2) FROM numbers(1000);

-- One part, so that the plan the decision is matched against does not depend on how the insert was split.
OPTIMIZE TABLE t_autopr_unpaired_sets FINAL;

-- With one reading thread the cost model sees replicas as a win on a table of this size, which is what
-- makes the optimization actually rewrite the query rather than decline.
SET max_threads = 1;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The decision needs statistics from an earlier execution, so the first run of each query only collects
-- them, and the second one can switch.

-- The parallel-replicas plan is built from the query tree shipped to the replicas, where constants longer
-- than `optimize_const_name_size` are replaced with `__getScalar` calls, also inside an `IN` subquery.
-- That changes the subquery's hash, which keys its set.
SELECT g, sumIf(v, k IN (SELECT k FROM t_autopr_unpaired_sets WHERE v < 3 AND k != repeat('x', 300)))
FROM t_autopr_unpaired_sets GROUP BY g ORDER BY g SETTINGS log_comment = '05401_long_warmup';
SELECT g, sumIf(v, k IN (SELECT k FROM t_autopr_unpaired_sets WHERE v < 3 AND k != repeat('x', 300)))
FROM t_autopr_unpaired_sets GROUP BY g ORDER BY g SETTINGS log_comment = '05401_long_second';

-- The same with `optimize_const_name_size = 0`, which replaces every constant.
SELECT g, sumIf(v, k IN (SELECT k FROM t_autopr_unpaired_sets WHERE v < 3 AND k != 'short'))
FROM t_autopr_unpaired_sets GROUP BY g ORDER BY g SETTINGS optimize_const_name_size = 0, log_comment = '05401_all_warmup';
SELECT g, sumIf(v, k IN (SELECT k FROM t_autopr_unpaired_sets WHERE v < 3 AND k != 'short'))
FROM t_autopr_unpaired_sets GROUP BY g ORDER BY g SETTINGS optimize_const_name_size = 0, log_comment = '05401_all_second';

-- `query_plan_convert_join_to_in` keys the set it makes of the right table by a random UUID, so the two
-- plans never hash it alike. The query must still return its result, with or without switching.
SELECT t.v, count() FROM t_autopr_unpaired_sets AS t INNER JOIN r_autopr_unpaired_sets AS r ON t.k = r.k
WHERE t.v < 5 GROUP BY t.v ORDER BY t.v
SETTINGS query_plan_convert_join_to_in = 1, query_plan_join_swap_table = 'false', log_comment = '05401_join_warmup';
SELECT t.v, count() FROM t_autopr_unpaired_sets AS t INNER JOIN r_autopr_unpaired_sets AS r ON t.k = r.k
WHERE t.v < 5 GROUP BY t.v ORDER BY t.v
SETTINGS query_plan_convert_join_to_in = 1, query_plan_join_swap_table = 'false', log_comment = '05401_join_second';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Keeps the check honest: a query declined for another reason would pass the checks above without ever
-- pairing the sets.
SELECT
    log_comment,
    ProfileEvents['ParallelReplicasUsedCount'] > 0 AS switched,
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] > 0 AS plans_not_comparable
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND event_date >= yesterday() AND log_comment IN ('05401_long_second', '05401_all_second')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_unpaired_sets;
DROP TABLE r_autopr_unpaired_sets;
