-- Only statistics covering every selected part may affect an estimate.
-- Tags: no-random-settings

SET allow_statistics = 1;
SET enable_analyzer = 1;
SET mutations_sync = 2, alter_sync = 2;
SET explain_query_plan_default = 'legacy';
SET use_statistics = 1,
    use_statistics_for_part_pruning = 0,
    use_query_condition_cache = 0,
    enable_cascades_optimizer = 0,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    query_plan_optimize_join_order_limit = 10,
    query_plan_optimize_join_order_randomize = 0,
    query_plan_optimize_join_order_algorithm = 'greedy',
    query_plan_join_swap_table = 0,
    use_hash_table_stats_for_join_reordering = 0,
    collect_hash_table_stats_during_joins = 0,
    max_insert_threads = 1;

DROP TABLE IF EXISTS t_incomplete_statistics;
DROP TABLE IF EXISTS d_incomplete_statistics;

CREATE TABLE t_incomplete_statistics
(
    p UInt8,
    id UInt64,
    v UInt64 STATISTICS(basic),
    y UInt8 STATISTICS(basic)
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS auto_statistics_types = '';

CREATE TABLE d_incomplete_statistics (id UInt64)
ENGINE = MergeTree
ORDER BY id
SETTINGS auto_statistics_types = '';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_incomplete_statistics SELECT 0, number, number, number % 2 FROM numbers(1000);
INSERT INTO t_incomplete_statistics SELECT 1, number + 1000, number + 1000, number % 2 FROM numbers(1000);
INSERT INTO d_incomplete_statistics SELECT number FROM numbers(2000);

-- Each column has statistics in only one of the two parts. Neither may be used
-- for a whole-table estimate; the result must match the statistics-disabled plan.
ALTER TABLE t_incomplete_statistics CLEAR STATISTICS v IN PARTITION 1;
ALTER TABLE t_incomplete_statistics CLEAR STATISTICS y IN PARTITION 0;

SELECT 'v and y are each incomplete',
    countIf(column = 'v') = 2
        AND countIf(column = 'v' AND length(statistics) > 0) = 1
        AND countIf(column = 'y') = 2
        AND countIf(column = 'y' AND length(statistics) > 0) = 1
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 't_incomplete_statistics'
    AND active;

SELECT 'all-incomplete estimate matches fallback',
    count() = 2 AND uniqExact(relation) = 1 AND countIf(notEmpty(relation)) = 2
FROM
(
    SELECT extract(explain, '(t_incomplete_statistics\\[[^]]+\\])') AS relation
    FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT count()
        FROM t_incomplete_statistics
        INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
        WHERE t_incomplete_statistics.v >= 1500 AND t_incomplete_statistics.y = 0
    )
    WHERE explain LIKE '%Join:%'
    UNION ALL
    SELECT extract(explain, '(t_incomplete_statistics\\[[^]]+\\])') AS relation
    FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT count()
        FROM t_incomplete_statistics
        INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
        WHERE t_incomplete_statistics.v >= 1500 AND t_incomplete_statistics.y = 0
        SETTINGS use_statistics = 0
    )
    WHERE explain LIKE '%Join:%'
);

-- Partition pruning leaves p=0's complete v statistics in scope even though v
-- is incomplete over the whole table. The p predicate remains in the filter,
-- so its default equality selectivity also affects the estimate (about 5 rows).
SELECT 'selected-partition v estimate is about 5',
    count() = 1
        AND countIf(toUInt64OrNull(extract(relation, 't_incomplete_statistics\\[[^0-9]*([0-9]+)\\]')) BETWEEN 4 AND 6) = 1
FROM
(
    SELECT extract(explain, '(t_incomplete_statistics\\[[^]]+\\])') AS relation
    FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT count()
        FROM t_incomplete_statistics
        INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
        WHERE t_incomplete_statistics.p = 0 AND t_incomplete_statistics.v >= 500
    )
    WHERE explain LIKE '%Join:%'
);

-- Restore y only. Incomplete v must fall back, while complete y still halves
-- the estimate: 2000 * 0.33 * 0.5 is about 330 rows (actual result: 250).
ALTER TABLE t_incomplete_statistics MATERIALIZE STATISTICS y IN PARTITION 0;

SELECT 'v remains partial while y is complete',
    countIf(column = 'v' AND length(statistics) > 0) = 1
        AND countIf(column = 'y' AND length(statistics) > 0) = 2
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 't_incomplete_statistics'
    AND active;

SELECT 'partial v falls back while retaining y',
    count() = 1
        AND uniqExact(relation) = 1
        AND countIf(toUInt64OrNull(extract(relation, 't_incomplete_statistics\\[[^0-9]*([0-9]+)\\]')) BETWEEN 300 AND 360) = 1
FROM
(
    SELECT extract(explain, '(t_incomplete_statistics\\[[^]]+\\])') AS relation
    FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT count()
        FROM t_incomplete_statistics
        INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
        WHERE t_incomplete_statistics.v >= 1500 AND t_incomplete_statistics.y = 0
    )
    WHERE explain LIKE '%Join:%'
);

SELECT 'mixed-state SELECT result is 250', count() = 250
FROM t_incomplete_statistics
INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
WHERE t_incomplete_statistics.v >= 1500 AND t_incomplete_statistics.y = 0;

ALTER TABLE t_incomplete_statistics MATERIALIZE STATISTICS v IN PARTITION 1;

SELECT 'v and y statistics cover both parts',
    countIf(column = 'v' AND length(statistics) > 0) = 2
        AND countIf(column = 'y' AND length(statistics) > 0) = 2
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 't_incomplete_statistics'
    AND active;

SELECT 'complete v estimate is about 500',
    count() = 1
        AND countIf(toUInt64OrNull(extract(relation, 't_incomplete_statistics\\[[^0-9]*([0-9]+)\\]')) BETWEEN 450 AND 550) = 1
FROM
(
    SELECT extract(explain, '(t_incomplete_statistics\\[[^]]+\\])') AS relation
    FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT count()
        FROM t_incomplete_statistics
        INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
        WHERE t_incomplete_statistics.v >= 1500
    )
    WHERE explain LIKE '%Join:%'
);

SELECT 'complete-state SELECT result is 500', count() = 500
FROM t_incomplete_statistics
INNER JOIN d_incomplete_statistics ON d_incomplete_statistics.id = t_incomplete_statistics.id
WHERE t_incomplete_statistics.v >= 1500;

DROP TABLE d_incomplete_statistics;
DROP TABLE t_incomplete_statistics;
