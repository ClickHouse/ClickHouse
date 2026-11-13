-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN projections output is missing on the initiator under parallel replicas.
-- A filtering projection that already removed a part stays used when a full projection replaces the
-- remaining read, and EXPLAIN keeps that part pruning on the surviving read. A filtering projection
-- whose rows are later read from a full projection does not: `force_optimize_projection_name`
-- follows that final plan.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projection_filtering = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 1;
SET min_table_rows_to_use_projection_index = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_proj_filter_part;

CREATE TABLE t_proj_filter_part (id UInt64, p UInt64, k UInt64, x UInt64, v String)
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

-- p_off exists only on partition 0 (k = 2). p_full exists only on partition 1 (k = 1).
-- Each side is one mark, so p_full is chosen because ORDER BY x matches its key.
-- The unrestricted plan replaces the parent read; part pruning must stay visible there.
INSERT INTO t_proj_filter_part SELECT number, 0, 2, cityHash64(number), toString(number) FROM numbers(1000);
INSERT INTO t_proj_filter_part SELECT number, 1, 1, cityHash64(number), toString(number) FROM numbers(1000);
ALTER TABLE t_proj_filter_part ADD PROJECTION p_off (SELECT _part_offset ORDER BY k);
ALTER TABLE t_proj_filter_part MATERIALIZE PROJECTION p_off IN PARTITION 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_proj_filter_part ADD PROJECTION p_full (SELECT k, x, v ORDER BY x);
ALTER TABLE t_proj_filter_part MATERIALIZE PROJECTION p_full IN PARTITION 1 SETTINGS mutations_sync = 2;

SELECT '-- pruned part still counts when the full projection replaces the read';

SELECT countIf(line = 'ReadFromMergeTree (p_full)')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN SELECT v FROM t_proj_filter_part WHERE k = 1 ORDER BY x LIMIT 1)
);

SELECT countIf(line IN (
    'Name: p_off',
    'Description: Projection has been analyzed and is used for part-level filtering',
    'Filtered Parts: 1'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM
    (
        EXPLAIN projections = 1
        SELECT v FROM t_proj_filter_part WHERE k = 1 ORDER BY x LIMIT 1
    )
);

SELECT countIf(line IN (
    'Projection',
    'Name: p_off',
    'Description: Projection has been analyzed and is used for part-level filtering'))
FROM
(
    SELECT trimLeft(explain) AS line
    FROM
    (
        EXPLAIN indexes = 1
        SELECT v FROM t_proj_filter_part WHERE k = 1 ORDER BY x LIMIT 1
    )
);

SELECT
    (
        SELECT v FROM t_proj_filter_part WHERE k = 1 ORDER BY x LIMIT 1
        SETTINGS optimize_use_projections = 0
    )
    =
    (
        SELECT v FROM t_proj_filter_part WHERE k = 1 ORDER BY x LIMIT 1
        SETTINGS force_optimize_projection_name = 'p_off'
    );

DROP TABLE t_proj_filter_part;

-- Both projections exist only on partition 0, which holds both k values. Partition 1 matches the
-- filter and has neither projection, so the parent read survives. p_full serves partition 0, and
-- p_off's row filter never runs.
DROP TABLE IF EXISTS t_proj_filter_row;

CREATE TABLE t_proj_filter_row (id UInt64, p UInt64, k UInt64, v String)
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

INSERT INTO t_proj_filter_row SELECT number, 0, number % 2, toString(number) FROM numbers(100000);
INSERT INTO t_proj_filter_row SELECT number + 100000, 1, 0, toString(number + 100000) FROM numbers(1000);
ALTER TABLE t_proj_filter_row ADD PROJECTION p_off (SELECT _part_offset ORDER BY k);
ALTER TABLE t_proj_filter_row MATERIALIZE PROJECTION p_off IN PARTITION 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_proj_filter_row ADD PROJECTION p_full (SELECT k, v ORDER BY k);
ALTER TABLE t_proj_filter_row MATERIALIZE PROJECTION p_full IN PARTITION 0 SETTINGS mutations_sync = 2;

SELECT '-- row filter does not survive';

SELECT countIf(line = 'Description: Projection has been analyzed and will be applied during reading')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM
    (
        EXPLAIN projections = 1
        SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0
        SETTINGS preferred_optimize_projection_name = 'p_off'
    )
);

SELECT countIf(line = 'Name: p_off')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM
    (
        EXPLAIN indexes = 1
        SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0
        SETTINGS preferred_optimize_projection_name = 'p_off'
    )
);

SELECT countIf(line = 'Description: Projection has been analyzed and will be applied during reading')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN projections = 1 SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0)
);

SELECT countIf(line = 'Description: Projection has been analyzed, but it does not filter the final read')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN projections = 1 SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0)
);

SELECT countIf(line = 'Name: p_off')
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN indexes = 1 SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0)
);

SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0 SETTINGS optimize_use_projections = 0;
SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0
SETTINGS preferred_optimize_projection_name = 'p_off', force_optimize_projection_name = 'p_off';
SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0 SETTINGS force_optimize_projection_name = 'p_full';
SELECT sum(length(v)) FROM t_proj_filter_row WHERE k = 0 SETTINGS force_optimize_projection_name = 'p_off'; -- { serverError INCORRECT_DATA }

DROP TABLE t_proj_filter_row;
