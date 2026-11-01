-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: checks the index analysis in EXPLAIN, which parallel replicas move to the coordinator.

-- With FINAL, the columns identical in all versions of a row (here: all but `score`, see `columns_varying_across_versions`)
-- are treated like the sorting key: conditions on them move to PREWHERE, skip indexes on them do not extend the ranges,
-- and partitions are pruned by them.

SET explain_query_plan_default = 'legacy';
SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, apply_prewhere_after_final = 0;
SET use_skip_indexes = 1, use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1, use_skip_indexes_on_data_read = 0;
SET do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SET defer_partition_pruning_after_final = 1, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_identical;

-- `month` and `mode` are functions of `id`, so they are identical in all versions of a row.
CREATE TABLE t_plain
(
    id UInt64,
    month UInt8,
    mode LowCardinality(String),
    score UInt32,
    score_bucket UInt32 MATERIALIZED intDiv(score, 100),
    version UInt64,
    deleted UInt8,
    INDEX idx_mode mode TYPE set(10) GRANULARITY 1,
    INDEX idx_score score TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version, deleted)
PARTITION BY month
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 1000000000,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;

CREATE TABLE t_identical AS t_plain;
ALTER TABLE t_identical MODIFY SETTING columns_varying_across_versions = 'score';

SYSTEM STOP MERGES t_plain;
SYSTEM STOP MERGES t_identical;

-- Version 1 of every row, then version 2 of every second row with a new score, some of them deleted.
-- Rows 0..333 are in month 1, 334..667 in month 2, 668..999 in month 3; `mode` is 'b' for the rows 900..999.
INSERT INTO t_plain SELECT number, 1 + intDiv(number, 334), if(number >= 900, 'b', 'a'), number, 1, 0 FROM numbers(1000);
INSERT INTO t_plain SELECT number * 2, 1 + intDiv(number * 2, 334), if(number * 2 >= 900, 'b', 'a'), 5000 + number, 2, number % 7 = 0 FROM numbers(500);
INSERT INTO t_identical SELECT * FROM t_plain WHERE version = 1;
INSERT INTO t_identical SELECT * FROM t_plain WHERE version = 2;

SELECT 'equal results',
    (SELECT groupArraySorted(1000)((id, score)) FROM t_plain FINAL WHERE mode = 'b' AND month = 3)
    = (SELECT groupArraySorted(1000)((id, score)) FROM t_identical FINAL WHERE mode = 'b' AND month = 3);

-- `score < 950` matches the replaced versions of the even rows: the index on `score` must not hide their replacements.
SELECT 'equal results with varying column',
    (SELECT groupArraySorted(1000)((id, score)) FROM t_plain FINAL WHERE mode = 'b' AND score < 950)
    = (SELECT groupArraySorted(1000)((id, score)) FROM t_identical FINAL WHERE mode = 'b' AND score < 950),
    (SELECT count() FROM t_identical FINAL WHERE mode = 'b' AND score < 950);

SELECT 'prewhere', countIf(explain LIKE '%Prewhere filter column%') FROM (EXPLAIN actions = 1 SELECT id FROM t_identical FINAL WHERE mode = 'b');

-- A column computed from a varying column varies as well.
SELECT 'no prewhere for column computed from varying', countIf(explain LIKE '%Prewhere filter column%') FROM (EXPLAIN actions = 1 SELECT id FROM t_identical FINAL WHERE score_bucket = 9);

SELECT 'no range extension', countIf(explain LIKE '%PrimaryKeyExpand%') FROM (EXPLAIN indexes = 1 SELECT id FROM t_identical FINAL WHERE mode = 'b');

-- With an index on a varying column as well, the ranges are extended only within the 4 granules left by the index on `mode`
-- (the last `Granules` line is the one of `PrimaryKeyExpand`).
SELECT 'range extension within mode', arrayElement(groupArray(trimLeft(explain)), -1)
FROM (EXPLAIN indexes = 1 SELECT id FROM t_identical FINAL WHERE mode = 'b' AND score >= 5300)
WHERE explain LIKE '%Granules%';

SELECT 'partition pruning', countIf(explain LIKE '%Parts: 2/6%') FROM (EXPLAIN indexes = 1 SELECT id FROM t_identical FINAL WHERE month = 3);

DROP TABLE t_plain;
DROP TABLE t_identical;
