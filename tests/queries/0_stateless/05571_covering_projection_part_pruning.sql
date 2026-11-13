-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN projections/indexes output is missing on the initiator under parallel replicas.
-- An index-only projection can cover the query (e.g. `sum(k)` against `ORDER BY k`) and still prune
-- parent parts. That pruning must satisfy `force_optimize_projection_name` and appear in
-- `EXPLAIN indexes = 1` even when the covering candidate is later rejected as a read.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projection_filtering = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET min_table_rows_to_use_projection_index = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_covering_proj_prune;

CREATE TABLE t_covering_proj_prune (id UInt64, p UInt64, k UInt64, v String)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    allow_part_offset_column_in_projections = 1,
    index_granularity = 8192,
    index_granularity_bytes = 0,
    min_index_granularity_bytes = 0,
    use_const_adaptive_granularity = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_covering_proj_prune SELECT number, 0, 2, toString(number) FROM numbers(1000);
INSERT INTO t_covering_proj_prune SELECT number, 1, 1, toString(number) FROM numbers(1000);
ALTER TABLE t_covering_proj_prune ADD PROJECTION p_off (SELECT _part_offset ORDER BY k);
ALTER TABLE t_covering_proj_prune MATERIALIZE PROJECTION p_off IN PARTITION 0 SETTINGS mutations_sync = 2;

SELECT '-- non-covering: missing v, filtering path';

SELECT countIf(line IN (
    'Name: p_off',
    'Description: Projection has been analyzed and is used for part-level filtering',
    'Filtered Parts: 1'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN projections = 1 SELECT sum(length(v)) FROM t_covering_proj_prune WHERE k = 1)
);

SELECT countIf(line IN (
    'Projection',
    'Name: p_off',
    'Description: Projection has been analyzed and is used for part-level filtering'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN indexes = 1 SELECT sum(length(v)) FROM t_covering_proj_prune WHERE k = 1)
);

SELECT
    (
        SELECT sum(length(v)) FROM t_covering_proj_prune WHERE k = 1
        SETTINGS optimize_use_projections = 0
    )
    =
    (
        SELECT sum(length(v)) FROM t_covering_proj_prune WHERE k = 1
        SETTINGS force_optimize_projection_name = 'p_off'
    );

SELECT '-- covering: has k, candidate rejected as a read, pruning still counts';

-- EXPLAIN projections keeps the cost-reject description on the covering candidate, but the prune
-- must still be visible. The part-level-filtering wording is on the index entry instead.
SELECT countIf(line IN (
    'Name: p_off',
    'Filtered Parts: 1'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN projections = 1 SELECT sum(k) FROM t_covering_proj_prune WHERE k = 1)
);

SELECT countIf(line IN (
    'Projection',
    'Name: p_off',
    'Description: Projection has been analyzed and is used for part-level filtering'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN indexes = 1 SELECT sum(k) FROM t_covering_proj_prune WHERE k = 1)
);

SELECT
    (
        SELECT sum(k) FROM t_covering_proj_prune WHERE k = 1
        SETTINGS optimize_use_projections = 0
    )
    =
    (
        SELECT sum(k) FROM t_covering_proj_prune WHERE k = 1
        SETTINGS force_optimize_projection_name = 'p_off'
    );

DROP TABLE t_covering_proj_prune;
