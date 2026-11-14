-- Tags: no-parallel-replicas

-- A projection whose sort order serves `ORDER BY ... LIMIT` stays selected when a deferred skip index of the
-- base table prunes some granules (issue #130115).

SET optimize_use_projections = 1;
SET optimize_read_in_order = 1;
SET use_query_condition_cache = 0;
SET optimize_projection_skip_index_ratio = 0.5;
SET explain_query_plan_default = 'legacy';
-- With `read_overflow_mode = 'throw'`, the `max_rows_to_read` of the CI profile disables the deferral of skip indexes.
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS t_proj_topn;

CREATE TABLE t_proj_topn
(
    id UInt64,
    b UInt64,
    c UInt64,
    INDEX c_idx c TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT * ORDER BY b)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, use_const_adaptive_granularity = 1;

INSERT INTO t_proj_topn SELECT number, cityHash64(number) % 1000000, number FROM numbers(10000);

-- With a LIMIT the projection is read in order and stops early.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT b FROM t_proj_topn WHERE c < 9000 ORDER BY b LIMIT 10 SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%ReadFromMergeTree%';

-- The primary-key-only comparison, as with `compatibility` before 26.11.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT b FROM t_proj_topn WHERE c < 9000 ORDER BY b LIMIT 10 SETTINGS use_skip_indexes_on_data_read = 1, optimize_projection_skip_index_ratio = 1
) WHERE explain ILIKE '%ReadFromMergeTree%';

-- Without a LIMIT, or with a LIMIT that reads till the end, the whole projection is read, so the skip-index-refined
-- base read wins.
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT b FROM t_proj_topn WHERE c < 9000 ORDER BY b SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT trimLeft(explain) FROM (
    EXPLAIN SELECT b FROM t_proj_topn WHERE c < 9000 ORDER BY b LIMIT 10 SETTINGS use_skip_indexes_on_data_read = 1, exact_rows_before_limit = 1
) WHERE explain ILIKE '%ReadFromMergeTree%';

SELECT b FROM t_proj_topn WHERE c < 9000 ORDER BY b LIMIT 3;

DROP TABLE t_proj_topn;
