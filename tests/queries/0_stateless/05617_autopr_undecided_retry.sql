-- Random settings limits: index_granularity=(8192, None); index_granularity_bytes=(10485760, None)
-- The adoption case reads a whole table in one thread with tiny read tasks: small granules make it slow.

-- Automatic parallel replicas skips building the plan with parallel replicas for a number of executions of a plan whose
-- last build ended without a decision. After the first such build of a plan, the next execution builds it again at once:
-- the first execution of a join measures the hash tables that join reordering takes row counts from.

DROP TABLE IF EXISTS t_undecided_l;
DROP TABLE IF EXISTS t_undecided_o;
DROP TABLE IF EXISTS t_undecided_wc;
DROP TABLE IF EXISTS t_undecided_wo;
DROP TABLE IF EXISTS t_undecided_wl;
DROP TABLE IF EXISTS t_undecided_s;

CREATE TABLE t_undecided_l (l_orderkey UInt64, l_price Float64, l_discount Float64) ENGINE = MergeTree ORDER BY l_orderkey;
CREATE TABLE t_undecided_o (o_orderkey UInt64, o_total Float64) ENGINE = MergeTree ORDER BY o_orderkey;
INSERT INTO t_undecided_l SELECT number % 10000, number % 1000, (number % 10) / 100 FROM numbers(100000);
INSERT INTO t_undecided_o SELECT number, number % 1000 FROM numbers(10000);

-- Without column statistics, so that the row estimates of a filtered read come from index analysis alone, which the
-- granularity decides.
CREATE TABLE t_undecided_wc (c_custkey UInt32, c_nationkey UInt8) ENGINE = MergeTree ORDER BY c_custkey
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_undecided_wo (o_orderkey UInt32, o_custkey UInt32, o_orderdate Date) ENGINE = MergeTree ORDER BY (o_orderdate, o_orderkey)
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_undecided_wl (l_orderkey UInt32, l_price Float64) ENGINE = MergeTree ORDER BY l_orderkey
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_undecided_wc SELECT number, number % 25 FROM numbers(15000) SETTINGS materialize_statistics_on_insert = 0;
INSERT INTO t_undecided_wo SELECT number, number % 15000, toDate('1992-01-01') + number % 2400 FROM numbers(150000)
SETTINGS materialize_statistics_on_insert = 0;
INSERT INTO t_undecided_wl SELECT number % 150000, number % 1000 FROM numbers(600000) SETTINGS materialize_statistics_on_insert = 0;

CREATE TABLE t_undecided_s (k String, v UInt64, g UInt8) ENGINE = MergeTree ORDER BY g;
INSERT INTO t_undecided_s SELECT toString(number % 2000), number % 1000, number % 2 FROM numbers(1000000);
OPTIMIZE TABLE t_undecided_s FINAL;

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0,
    automatic_parallel_replicas_undecided_retry_after_executions = 3;
-- What the joins below are ordered by, and whether their hash tables are measured, must not vary.
SET join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1,
    query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10, query_plan_join_swap_table = 'auto',
    max_threads = 4;

-- `LIMIT` without `ORDER BY`: the plans cannot be compared. The second run builds the plan again at once, the next
-- three skip it, and the sixth builds it again.
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_1';
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_2';
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_3';
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_4';
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_5';
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_6';

-- The same with skipping turned off: every run builds the plan, the third too, which would skip otherwise.
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_off_1', automatic_parallel_replicas_undecided_retry_after_executions = 0;
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_off_2', automatic_parallel_replicas_undecided_retry_after_executions = 0;
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_off_3', automatic_parallel_replicas_undecided_retry_after_executions = 0;

-- The largest value skips the build for good after the second outcome, but the second run still builds it.
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_max_1', automatic_parallel_replicas_undecided_retry_after_executions = 18446744073709551615;
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_max_2', automatic_parallel_replicas_undecided_retry_after_executions = 18446744073709551615;
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_max_3', automatic_parallel_replicas_undecided_retry_after_executions = 18446744073709551615;

-- A correlated subquery: the planner uses no parallel replicas in the plan built with them. The second run builds the
-- plan again at once, the third skips it.
SELECT sum(l_price) FROM t_undecided_l, t_undecided_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_undecided_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'undecided_correlated_1';
SELECT sum(l_price) FROM t_undecided_l, t_undecided_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_undecided_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'undecided_correlated_2';
SELECT sum(l_price) FROM t_undecided_l, t_undecided_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_undecided_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'undecided_correlated_3';

-- The build side of the join is empty, so its probe side, the read the replicas would split, is never read, and the
-- collection measures nothing. The second run builds the plan again at once, the next two skip it.
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_1';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_2';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_3';
SELECT o_total, sum(l_price) FROM t_undecided_l INNER JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE o_total = 0.5
GROUP BY o_total FORMAT Null SETTINGS log_comment = 'undecided_empty_4';

-- Index analysis selects no rows, so the plans are not compared. Without remembered boundaries this is found only
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

-- Without column statistics the plan with parallel replicas has no row estimates for the filtered read, so its joins
-- are ordered differently and the two plans cannot be compared. The first run measures the hash tables; with their
-- sizes, the second, which builds the plan again at once, orders both plans alike, matches them and collects
-- statistics. The third decides from them.
SELECT c_nationkey, sum(l_price) FROM t_undecided_wc, t_undecided_wo, t_undecided_wl
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1992-03-01'
GROUP BY c_nationkey FORMAT Null SETTINGS log_comment = 'undecided_warm_1';
SELECT c_nationkey, sum(l_price) FROM t_undecided_wc, t_undecided_wo, t_undecided_wl
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1992-03-01'
GROUP BY c_nationkey FORMAT Null SETTINGS log_comment = 'undecided_warm_2';
SELECT c_nationkey, sum(l_price) FROM t_undecided_wc, t_undecided_wo, t_undecided_wl
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1992-03-01'
GROUP BY c_nationkey FORMAT Null SETTINGS log_comment = 'undecided_warm_3';

-- Statistics that favour parallel replicas do not make the plan usable: as in `05401_autopr_set_transplant_unpaired_sets`,
-- the set of the `IN` subquery has no counterpart in the plan with parallel replicas, so adopting it fails after the
-- cost model, every time. The first run collects statistics, the second fails to adopt, the third builds the plan
-- again at once and fails the same way, and the fourth skips it even though the statistics still favour it.
SELECT g, sumIf(v, k IN (SELECT k FROM t_undecided_s WHERE v < 3 AND k != repeat('x', 300)))
FROM t_undecided_s GROUP BY g FORMAT Null
SETTINGS max_threads = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1024, parallel_replicas_plan_based = 0,
    log_comment = 'undecided_adopt_1';
SELECT g, sumIf(v, k IN (SELECT k FROM t_undecided_s WHERE v < 3 AND k != repeat('x', 300)))
FROM t_undecided_s GROUP BY g FORMAT Null
SETTINGS max_threads = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1024, parallel_replicas_plan_based = 0,
    log_comment = 'undecided_adopt_2';
SELECT g, sumIf(v, k IN (SELECT k FROM t_undecided_s WHERE v < 3 AND k != repeat('x', 300)))
FROM t_undecided_s GROUP BY g FORMAT Null
SETTINGS max_threads = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1024, parallel_replicas_plan_based = 0,
    log_comment = 'undecided_adopt_3';
SELECT g, sumIf(v, k IN (SELECT k FROM t_undecided_s WHERE v < 3 AND k != repeat('x', 300)))
FROM t_undecided_s GROUP BY g FORMAT Null
SETTINGS max_threads = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1024, parallel_replicas_plan_based = 0,
    log_comment = 'undecided_adopt_4';

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
DROP TABLE t_undecided_wc;
DROP TABLE t_undecided_wo;
DROP TABLE t_undecided_wl;
DROP TABLE t_undecided_s;
