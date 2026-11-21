DROP TABLE IF EXISTS t_union_agg_left;
DROP TABLE IF EXISTS t_union_agg_right;
DROP TABLE IF EXISTS t_union_agg_unique;

CREATE TABLE t_union_agg_left (left_key UInt32, left_value Nullable(Int64)) ENGINE = MergeTree ORDER BY left_key SETTINGS index_granularity = 8192, auto_statistics_types = '';
CREATE TABLE t_union_agg_right (right_key UInt32, right_value Nullable(Int64)) ENGINE = MergeTree ORDER BY right_key SETTINGS index_granularity = 8192, auto_statistics_types = '';
CREATE TABLE t_union_agg_unique (unique_key UInt32, unique_value Int64) ENGINE = MergeTree ORDER BY unique_key SETTINGS index_granularity = 8192, auto_statistics_types = '';
INSERT INTO t_union_agg_left SELECT number % 10, if(number % 1000 = 0, NULL, number) FROM numbers(1000000);
INSERT INTO t_union_agg_right SELECT 5 + number % 10, -toInt64(number) FROM numbers(1000000);
INSERT INTO t_union_agg_unique SELECT number, number FROM numbers(1000000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_union_agg_left": {"cardinality": 1000000, "avg_row_bytes": 12, "distinct_keys": {"left_key": 10}}, "t_union_agg_right": {"cardinality": 1000000, "avg_row_bytes": 12, "distinct_keys": {"right_key": 10}}, "t_union_agg_unique": {"cardinality": 1000000, "avg_row_bytes": 12, "distinct_keys": {"unique_key": 1000000}}}';

SELECT '-- the union runs on every node, so the aggregation over it is split into partial and merge';
EXPLAIN SELECT key, sum(value) FROM
(
    SELECT left_key AS key, left_value AS value FROM t_union_agg_left
    UNION ALL
    SELECT right_key AS key, right_value AS value FROM t_union_agg_right
)
GROUP BY key;

SELECT '-- overlapping keys, NULL values and several aggregates';
SELECT key, sum(value), count(value), min(value), max(value), uniqExact(value) FROM
(
    SELECT left_key AS key, left_value AS value FROM t_union_agg_left
    UNION ALL
    SELECT right_key AS key, right_value AS value FROM t_union_agg_right
)
GROUP BY key ORDER BY key;

SELECT '-- partial states in bucket order reach a memory-efficient merge one stream per node';
SELECT count(), sum(s), sum(c) FROM
(
    SELECT key, sum(value) AS s, count() AS c FROM
    (
        SELECT left_key AS key, left_value AS value FROM t_union_agg_left
        UNION ALL
        SELECT right_key AS key, right_value AS value FROM t_union_agg_right
    )
    GROUP BY key
)
SETTINGS distributed_aggregation_memory_efficient = 1, distributed_plan_partial_aggregation_before_shuffle = 0,
    group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

SELECT '-- UNION DISTINCT';
SELECT count(), sum(key) FROM
(
    SELECT left_key AS key FROM t_union_agg_left
    UNION DISTINCT
    SELECT right_key AS key FROM t_union_agg_right
);

SELECT '-- the union counts the rows of every branch: the plan does not depend on the branch order';
EXPLAIN SELECT key, sum(value) FROM
(
    SELECT left_key AS key, left_value AS value FROM t_union_agg_left
    UNION ALL
    SELECT unique_key AS key, unique_value AS value FROM t_union_agg_unique
)
GROUP BY key;
EXPLAIN SELECT key, sum(value) FROM
(
    SELECT unique_key AS key, unique_value AS value FROM t_union_agg_unique
    UNION ALL
    SELECT left_key AS key, left_value AS value FROM t_union_agg_left
)
GROUP BY key;

DROP TABLE t_union_agg_left;
DROP TABLE t_union_agg_right;
DROP TABLE t_union_agg_unique;
