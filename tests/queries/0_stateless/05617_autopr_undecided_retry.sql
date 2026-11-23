-- Random settings limits: enable_shared_storage_snapshot_in_query=(1, 1)
-- Automatic parallel replicas skips building the plan with parallel replicas for a number of executions of a plan whose
-- last build ended without a decision. After the first such build of a plan, the next execution builds it again at once:
-- the first execution of a join measures the hash tables that join reordering takes row counts from.

DROP TABLE IF EXISTS t_undecided_l;
DROP TABLE IF EXISTS t_undecided_o;
DROP TABLE IF EXISTS t_undecided_wd;
DROP TABLE IF EXISTS t_undecided_wt;

CREATE TABLE t_undecided_l (l_orderkey UInt64, l_price Float64) ENGINE = MergeTree ORDER BY l_orderkey;
CREATE TABLE t_undecided_o (o_orderkey UInt64, o_total Float64) ENGINE = MergeTree ORDER BY o_orderkey;
INSERT INTO t_undecided_l SELECT number % 10000, number % 1000 FROM numbers(100000);
INSERT INTO t_undecided_o SELECT number, number % 1000 FROM numbers(10000);

-- Column statistics on one side of the join only, so that a filtered read of the other is estimated from index analysis.
CREATE TABLE t_undecided_wd (id UInt64 STATISTICS(uniq), v UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_undecided_wt (id UInt64, k UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_undecided_wd SELECT number % 10000, 10000 + number % 5000 FROM numbers(300000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO t_undecided_wt SELECT number, 10000 + number % 20000, number FROM numbers(300000) SETTINGS materialize_statistics_on_insert = 0;

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0,
    automatic_parallel_replicas_undecided_retry_after_executions = 3;
-- What the joins below are ordered by, and whether their hash tables are measured, must not vary.
SET join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1,
    query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10, query_plan_join_swap_table = 'auto',
    max_threads = 4;

-- The build side of the join is empty, so its probe side, the read the replicas would split, is never read, and the
-- collection measures nothing. The second run builds the plan again at once, the next three skip it, and the sixth
-- builds it again.
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_1';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_2';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_3';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_4';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_5';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_6';

-- The same with skipping turned off: every run builds the plan, the third too, which would skip otherwise.
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_off_1', automatic_parallel_replicas_undecided_retry_after_executions = 0;
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_off_2', automatic_parallel_replicas_undecided_retry_after_executions = 0;
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_off_3', automatic_parallel_replicas_undecided_retry_after_executions = 0;

-- The largest value skips the build for good after the second outcome, but the second run still builds it.
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_max_1', automatic_parallel_replicas_undecided_retry_after_executions = 18446744073709551615;
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_max_2', automatic_parallel_replicas_undecided_retry_after_executions = 18446744073709551615;
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_max_3', automatic_parallel_replicas_undecided_retry_after_executions = 18446744073709551615;

-- Index analysis selects no rows, so there is nothing to decide. Without remembered boundaries this is found only
-- after building the plan with parallel replicas, and is deferred like the other outcomes: the second run builds the
-- plan again at once, the next two skip it.
SELECT sum(o_total) FROM t_undecided_o WHERE o_orderkey > 100000
FORMAT Null SETTINGS log_comment = 'undecided_no_rows_1', automatic_parallel_replicas_remember_boundary = 0;
SELECT sum(o_total) FROM t_undecided_o WHERE o_orderkey > 100000
FORMAT Null SETTINGS log_comment = 'undecided_no_rows_2', automatic_parallel_replicas_remember_boundary = 0;
SELECT sum(o_total) FROM t_undecided_o WHERE o_orderkey > 100000
FORMAT Null SETTINGS log_comment = 'undecided_no_rows_3', automatic_parallel_replicas_remember_boundary = 0;
SELECT sum(o_total) FROM t_undecided_o WHERE o_orderkey > 100000
FORMAT Null SETTINGS log_comment = 'undecided_no_rows_4', automatic_parallel_replicas_remember_boundary = 0;

-- The two subqueries are identical reads of the same table, so the reads of the plan with parallel replicas cannot be
-- paired with those of the single-node plan and take no row estimates from its index analysis. The joins of the two
-- plans are ordered differently, and the plans cannot be compared. The first run measures the hash tables; with their
-- sizes, the second, which builds the plan again at once, orders both plans alike, matches them and collects
-- statistics. The third decides from them: the read is too small to occupy more threads, so it declines. The CI
-- profile with automatic parallel replicas ignores that cap.
SET use_statistics = 1, enable_join_runtime_filters = 0, query_plan_propagate_predicate_across_join = 0,
    parallel_replicas_plan_based = 0, optimize_move_to_prewhere = 1, automatic_parallel_replicas_ignore_thresholds = 0;
SELECT count(), sum(d.v), sum(a.v), sum(b.v)
FROM t_undecided_wd AS d
INNER JOIN (SELECT id, k, v FROM t_undecided_wt WHERE id < 20000) AS a ON d.id = a.id
INNER JOIN (SELECT id, k, v FROM t_undecided_wt WHERE id < 20000) AS b ON d.v = b.k
WHERE b.id < 1000
FORMAT Null SETTINGS log_comment = 'undecided_warm_1';
SELECT count(), sum(d.v), sum(a.v), sum(b.v)
FROM t_undecided_wd AS d
INNER JOIN (SELECT id, k, v FROM t_undecided_wt WHERE id < 20000) AS a ON d.id = a.id
INNER JOIN (SELECT id, k, v FROM t_undecided_wt WHERE id < 20000) AS b ON d.v = b.k
WHERE b.id < 1000
FORMAT Null SETTINGS log_comment = 'undecided_warm_2';
SELECT count(), sum(d.v), sum(a.v), sum(b.v)
FROM t_undecided_wd AS d
INNER JOIN (SELECT id, k, v FROM t_undecided_wt WHERE id < 20000) AS a ON d.id = a.id
INNER JOIN (SELECT id, k, v FROM t_undecided_wt WHERE id < 20000) AS b ON d.v = b.k
WHERE b.id < 1000
FORMAT Null SETTINGS log_comment = 'undecided_warm_3';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasRetryDeferred'] AS deferred,
       ProfileEvents['AutoParallelReplicasPlanWithoutParallelReplicas'] AS without_parallel_replicas,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS decided
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `undecided_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'undecided_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_undecided_l;
DROP TABLE t_undecided_o;
DROP TABLE t_undecided_wd;
DROP TABLE t_undecided_wt;
