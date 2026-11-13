-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN output is missing on the initiator under parallel replicas.
-- A row-filtering projection can look used after the first rewrite, while a later rewrite of the
-- same parent read replaces those parts with another full projection. The final plan never reads
-- the row filter, so `force_optimize_projection_name` must not accept it.

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

DROP TABLE IF EXISTS t_proj_filter_rewrite;

CREATE TABLE t_proj_filter_rewrite (id UInt64, p UInt64, k UInt64, v String)
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

-- Both partitions hold every residue of k. p_off and p_full0 exist only on the smaller partition,
-- p_full1 only on the larger one. The first rewrite takes p_full1 and leaves the row filter on the
-- parent read; the second rewrite replaces that read with p_full0.
INSERT INTO t_proj_filter_rewrite SELECT number, 0, number % 16, toString(number) FROM numbers(16384);
INSERT INTO t_proj_filter_rewrite SELECT number + 16384, 1, number % 16, toString(number + 16384) FROM numbers(65536);
ALTER TABLE t_proj_filter_rewrite ADD PROJECTION p_off (SELECT _part_offset ORDER BY k);
ALTER TABLE t_proj_filter_rewrite MATERIALIZE PROJECTION p_off IN PARTITION 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_proj_filter_rewrite ADD PROJECTION p_full0 (SELECT k, v ORDER BY k);
ALTER TABLE t_proj_filter_rewrite MATERIALIZE PROJECTION p_full0 IN PARTITION 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_proj_filter_rewrite ADD PROJECTION p_full1 (SELECT k, v ORDER BY k);
ALTER TABLE t_proj_filter_rewrite MATERIALIZE PROJECTION p_full1 IN PARTITION 1 SETTINGS mutations_sync = 2;

SELECT '-- final plan is the two full projections';

SELECT countIf(line = 'ReadFromMergeTree (p_full0)')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0)
);

SELECT countIf(line = 'ReadFromMergeTree (p_full1)')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0)
);

SELECT countIf(line IN (
    'Name: p_off',
    'Description: Projection has been analyzed and will be applied during reading'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN indexes = 1 SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0)
);

SELECT
    (SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0 SETTINGS optimize_use_projections = 0)
    =
    (SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0);

SELECT
    (SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0 SETTINGS optimize_use_projections = 0)
    =
    (SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0 SETTINGS force_optimize_projection_name = 'p_full0');

SELECT
    (SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0 SETTINGS optimize_use_projections = 0)
    =
    (SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0 SETTINGS force_optimize_projection_name = 'p_full1');

SELECT sum(length(v)) FROM t_proj_filter_rewrite WHERE k = 0 SETTINGS force_optimize_projection_name = 'p_off'; -- { serverError INCORRECT_DATA }

DROP TABLE t_proj_filter_rewrite;
