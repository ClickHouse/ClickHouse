SET explain_query_plan_default = 'pretty';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 2;
SET collect_hash_table_stats_during_joins = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;

DROP TABLE IF EXISTS nested_join_width_wide;
DROP TABLE IF EXISTS nested_join_width_narrow;
CREATE TABLE nested_join_width_wide (k UInt64, payload String) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;
CREATE TABLE nested_join_width_narrow (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO nested_join_width_wide SELECT number, repeat('x', 160) FROM numbers(15000);
INSERT INTO nested_join_width_narrow SELECT number FROM numbers(1, 16000);

-- The outer join recursively optimizes the inner join before the traversal reaches its LIMIT.
-- The inner join must still build the smaller row count and defer payload until after LIMIT.
SELECT
    countIf(position(explain, 'JoinLazyColumnsStep') > 0),
    countIf(extract(explain, 'Type: (left|right)') = 'right'),
    countIf(extract(explain, 'Type: (left|right)') = 'left')
FROM (
    EXPLAIN SELECT s.k, s.payload
    FROM nested_join_width_narrow AS t
    INNER ALL JOIN (
        SELECT k, payload
        FROM nested_join_width_wide LEFT ALL JOIN nested_join_width_narrow USING (k)
        ORDER BY cityHash64(k) DESC LIMIT 10
    ) AS s USING (k)
);

-- When lazy materialization is disabled, the payload participates in the build-side cost.
SET query_plan_lazy_materialization_for_join = 0;
SELECT
    countIf(position(explain, 'JoinLazyColumnsStep') > 0),
    countIf(extract(explain, 'Type: (left|right)') = 'right'),
    countIf(extract(explain, 'Type: (left|right)') = 'left')
FROM (
    EXPLAIN SELECT s.k, s.payload
    FROM nested_join_width_narrow AS t
    INNER ALL JOIN (
        SELECT k, payload
        FROM nested_join_width_wide LEFT ALL JOIN nested_join_width_narrow USING (k)
        ORDER BY cityHash64(k) DESC LIMIT 10
    ) AS s USING (k)
);

DROP TABLE nested_join_width_wide;
DROP TABLE nested_join_width_narrow;
