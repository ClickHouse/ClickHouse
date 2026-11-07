-- A nested join that the join order optimizer planned as `full_sorting_merge` emits rows sorted by its keys even
-- when its own `join_algorithm` list does not contain a merge algorithm. The output order predicted for it must
-- include the planned algorithm, otherwise the runtime-filter pass plants a filter on the join above and erases
-- its eligible `sorted_merge`, forcing a hash join.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET optimize_read_in_order = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_join_swap_table = 'auto';
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_shard_by_pk_ranges = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;

CREATE TABLE a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO a SELECT number, number FROM numbers(100);
INSERT INTO b SELECT number, number * 10 FROM numbers(50, 100);
INSERT INTO c SELECT number, number * 100 FROM numbers(0, 120);

SET param__internal_join_table_stat_hints = '{
    "a": { "cardinality": 50000000, "distinct_keys": { "id": 50000000 } },
    "b": { "cardinality": 50000000, "distinct_keys": { "id": 50000000 } },
    "c": { "cardinality": 50000000, "distinct_keys": { "id": 50000000 } }
}';

-- Both joins are merge joins, and no runtime filter is built.
SELECT
    countIf(explain LIKE '%MergeJoinTransform%') AS merge_joins,
    countIf(explain LIKE '%JoiningTransform%') AS hash_joins,
    countIf(explain LIKE '%BuildRuntimeFilter%') AS runtime_filters
FROM
(
    EXPLAIN PIPELINE
    SELECT sum(s.v + c.v)
    FROM
    (
        SELECT b.id AS id, a.v AS v
        FROM a INNER JOIN b ON a.id = b.id
        SETTINGS join_algorithm = 'direct,hash,ie_join'
    ) AS s
    ANY RIGHT JOIN c ON s.id = c.id
    SETTINGS join_algorithm = 'sorted_merge,hash'
);

SELECT sum(s.v + c.v)
FROM
(
    SELECT b.id AS id, a.v AS v
    FROM a INNER JOIN b ON a.id = b.id
    SETTINGS join_algorithm = 'direct,hash,ie_join'
) AS s
ANY RIGHT JOIN c ON s.id = c.id
SETTINGS join_algorithm = 'sorted_merge,hash';

SELECT sum(s.v + c.v)
FROM
(
    SELECT b.id AS id, a.v AS v
    FROM a INNER JOIN b ON a.id = b.id
) AS s
ANY RIGHT JOIN c ON s.id = c.id
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_algorithm = 'greedy', enable_join_runtime_filters = 0;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
