-- ANY INNER JOIN uses each right row at most once over the whole left input, so a distributed plan
-- must not broadcast its right side: every left task would join each key again.

SET explain_query_plan_default = 'legacy';
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_default_reader_bucket_count = 2;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":1}';
SET param__internal_join_table_stat_hints = '{
    "t_any_inner_l": { "cardinality": 1000000, "avg_row_bytes": 16, "distinct_keys": { "k": 10 } },
    "t_any_inner_r": { "cardinality": 1000,    "avg_row_bytes": 16, "distinct_keys": { "k": 10 } }
}';

DROP TABLE IF EXISTS t_any_inner_l;
DROP TABLE IF EXISTS t_any_inner_r;

CREATE TABLE t_any_inner_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v
    SETTINGS index_granularity = 8192, auto_statistics_types = '';
CREATE TABLE t_any_inner_r (k UInt64 STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192;

-- Every part of the left table holds every key, so every left task sees every key.
SYSTEM STOP MERGES t_any_inner_l;
INSERT INTO t_any_inner_l SELECT number % 10, number FROM numbers(100);
INSERT INTO t_any_inner_l SELECT number % 10, number + 100 FROM numbers(100);
INSERT INTO t_any_inner_l SELECT number % 10, number + 200 FROM numbers(100);
INSERT INTO t_any_inner_l SELECT number % 10, number + 300 FROM numbers(100);
INSERT INTO t_any_inner_r SELECT number % 10, number FROM numbers(1000);

SELECT '-- rule-based';
-- ANY LEFT JOIN of the same tables is broadcast: the right side is small enough.
SELECT countIf(explain LIKE '%BroadcastExchange%') FROM (
    EXPLAIN SELECT count() FROM t_any_inner_l AS l ANY LEFT JOIN t_any_inner_r AS r ON l.k = r.k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0);
SELECT countIf(explain LIKE '%BroadcastExchange%') FROM (
    EXPLAIN SELECT count() FROM t_any_inner_l AS l ANY INNER JOIN t_any_inner_r AS r ON l.k = r.k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0);
SELECT count() FROM t_any_inner_l AS l ANY INNER JOIN t_any_inner_r AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_fallback_to_local_execution = 0;
SELECT count() FROM t_any_inner_l AS l ANY INNER JOIN (SELECT k FROM t_any_inner_r GROUP BY k) AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_fallback_to_local_execution = 0;

SELECT '-- Cascades';
SELECT countIf(explain LIKE '%Broadcast HashJoin%') FROM (
    EXPLAIN SELECT count() FROM t_any_inner_l AS l ANY LEFT JOIN t_any_inner_r AS r ON l.k = r.k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT countIf(explain LIKE '%Broadcast HashJoin%') FROM (
    EXPLAIN SELECT count() FROM t_any_inner_l AS l ANY INNER JOIN t_any_inner_r AS r ON l.k = r.k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT count() FROM t_any_inner_l AS l ANY INNER JOIN t_any_inner_r AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_fallback_to_local_execution = 0;

SELECT '-- single node';
SELECT count() FROM t_any_inner_l AS l ANY INNER JOIN t_any_inner_r AS r ON l.k = r.k SETTINGS make_distributed_plan = 0;

DROP TABLE t_any_inner_l;
DROP TABLE t_any_inner_r;
