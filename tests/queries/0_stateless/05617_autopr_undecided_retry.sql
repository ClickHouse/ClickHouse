-- Automatic parallel replicas skips building the plan with parallel replicas for a number of executions of a plan whose
-- last build ended without a decision, and builds it at once when a hash table statistic of its joins appears or
-- changes by more than twice.

DROP TABLE IF EXISTS t_undecided_l;
DROP TABLE IF EXISTS t_undecided_o;
DROP TABLE IF EXISTS t_undecided_wc;
DROP TABLE IF EXISTS t_undecided_wo;
DROP TABLE IF EXISTS t_undecided_wl;

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

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0,
    automatic_parallel_replicas_undecided_retry_after_executions = 3;
-- What the joins below are ordered by, and whether their hash tables are measured at all, must not vary.
SET join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1,
    query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10, query_plan_join_swap_table = 'auto',
    max_threads = 4;

-- `LIMIT` without `ORDER BY`: the plans cannot be compared. The next three runs skip the build, the fifth builds again.
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

-- The same with skipping turned off: every run builds the plan.
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_off_1', automatic_parallel_replicas_undecided_retry_after_executions = 0;
SELECT l_orderkey, l_price FROM t_undecided_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_limit_off_2', automatic_parallel_replicas_undecided_retry_after_executions = 0;

-- A correlated subquery: the planner uses no parallel replicas in the plan built with them. The query joins, and the
-- hash table its first run builds changes what the second sees, so the second builds the plan again and the third skips.
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

-- A join that cannot be compared either. Its first run is the first to build the hash table, so the second finds
-- statistics that the first did not, and builds the plan again. The third finds them as the second did, and skips.
SELECT l_orderkey, o_total FROM t_undecided_l JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_join_1';
SELECT l_orderkey, o_total FROM t_undecided_l JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_join_2';
SELECT l_orderkey, o_total FROM t_undecided_l JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_join_3';

-- A table inserted into all the time grows the hash table on every run, and a grown hash table replaces its stored
-- size, but only a size that moved by more than 2x counts as a change. The fourth run grows the stored sizes, and the
-- fifth, which sees them grown, still skips.
INSERT INTO t_undecided_l VALUES (1, 100, 0.01);
INSERT INTO t_undecided_o VALUES (100000, 5);
SELECT l_orderkey, o_total FROM t_undecided_l JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_join_4';
SELECT l_orderkey, o_total FROM t_undecided_l JOIN t_undecided_o ON o_orderkey = l_orderkey WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'undecided_join_5';

-- Without column statistics the plan with parallel replicas has no row estimates for the filtered read, so its joins
-- are ordered differently and the two plans cannot be compared. The first run measures the hash tables; with their
-- sizes, the second orders both plans alike under the same key as the first, builds the plan again and matches it,
-- and collects statistics. The third decides from them.
SELECT c_nationkey, sum(l_price) FROM t_undecided_wc, t_undecided_wo, t_undecided_wl
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1992-03-01'
GROUP BY c_nationkey FORMAT Null SETTINGS log_comment = 'undecided_warm_1';
SELECT c_nationkey, sum(l_price) FROM t_undecided_wc, t_undecided_wo, t_undecided_wl
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1992-03-01'
GROUP BY c_nationkey FORMAT Null SETTINGS log_comment = 'undecided_warm_2';
SELECT c_nationkey, sum(l_price) FROM t_undecided_wc, t_undecided_wo, t_undecided_wl
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '1992-03-01'
GROUP BY c_nationkey FORMAT Null SETTINGS log_comment = 'undecided_warm_3';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log, text_log;

SELECT q.log_comment,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasRetryDeferred'] AS deferred,
       ProfileEvents['AutoParallelReplicasPlanWithoutParallelReplicas'] AS without_parallel_replicas,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS decided,
       -- Built again because the hash table statistics changed, not because the plan got another key.
       t.messages AS statistics_changed
FROM system.query_log AS q
LEFT JOIN
(
    SELECT query_id, count() AS messages FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
      AND message LIKE 'The hash table statistics the joins were ordered by changed%'
    GROUP BY query_id
) AS t ON t.query_id = q.query_id
WHERE q.event_date >= yesterday() AND q.event_time >= now() - toIntervalMinute(15)
  AND q.current_database = currentDatabase() AND q.type = 'QueryFinish'
  -- `undecided_` cannot collide with the comment the test harness sets, which starts with the test name
  AND q.log_comment LIKE 'undecided_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND q.query_id = q.initial_query_id
ORDER BY q.log_comment;

DROP TABLE t_undecided_l;
DROP TABLE t_undecided_o;
DROP TABLE t_undecided_wc;
DROP TABLE t_undecided_wo;
DROP TABLE t_undecided_wl;
