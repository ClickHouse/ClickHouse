-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads; the merged part's data version asserted below depends on it
-- Random settings limits: materialize_statistics_on_insert=(1, 1)

-- A merge that runs while a DROP COLUMN or RENAME COLUMN is pending records it as applied to the merged part. The
-- merged part's statistics of a name that the pending command gave to another column must describe that column, not
-- the one stored under the name in the source part, otherwise statistics-based part pruning drops matching parts.
-- https://github.com/ClickHouse/ClickHouse/issues/134170

DROP TABLE IF EXISTS t_vertical;
DROP TABLE IF EXISTS t_horizontal;
DROP TABLE IF EXISTS t_chain;
DROP TABLE IF EXISTS t_nested;

-- `max_bytes_to_merge_at_max_space_in_pool = 1` leaves the merge to `OPTIMIZE ... FINAL`, so the asserted part is the result of that one merge.
CREATE TABLE t_vertical (k UInt64, c UInt64, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic', max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

CREATE TABLE t_horizontal (k UInt64, c UInt64, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic', max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_chain (k UInt64, a UInt64, b UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic', max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, enable_vertical_merge_algorithm = 0;

-- `DROP COLUMN n` drops `n.x` as well.
CREATE TABLE t_nested (k UInt64, `n.x` UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic', max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, enable_vertical_merge_algorithm = 0;

INSERT INTO t_vertical (k) SELECT number * 2 FROM numbers(100);
INSERT INTO t_vertical (k) SELECT number * 2 + 1 FROM numbers(100);
INSERT INTO t_horizontal (k) SELECT number * 2 FROM numbers(100);
INSERT INTO t_horizontal (k) SELECT number * 2 + 1 FROM numbers(100);
INSERT INTO t_chain SELECT number, number, number + 1000 FROM numbers(100);
INSERT INTO t_chain SELECT number + 100, number, number + 1000 FROM numbers(100);
INSERT INTO t_nested (k) SELECT number * 2 FROM numbers(100);
INSERT INTO t_nested (k) SELECT number * 2 + 1 FROM numbers(100);

-- Keep the mutations unselected, so the merges below straddle them.
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

ALTER TABLE t_vertical DROP COLUMN c, DROP COLUMN n SETTINGS alter_sync = 0;
ALTER TABLE t_vertical ADD COLUMN c UInt64 DEFAULT 42, ADD COLUMN n Nullable(UInt32) DEFAULT 5;
ALTER TABLE t_horizontal DROP COLUMN c, DROP COLUMN n SETTINGS alter_sync = 0;
ALTER TABLE t_horizontal ADD COLUMN c UInt64 DEFAULT 42, ADD COLUMN n Nullable(UInt32) DEFAULT 5;
ALTER TABLE t_chain RENAME COLUMN b TO c, RENAME COLUMN a TO b SETTINGS alter_sync = 0;
ALTER TABLE t_nested DROP COLUMN n SETTINGS alter_sync = 0;
ALTER TABLE t_nested ADD COLUMN `n.x` UInt64 DEFAULT 42;

OPTIMIZE TABLE t_vertical FINAL;
OPTIMIZE TABLE t_horizontal FINAL;
OPTIMIZE TABLE t_chain FINAL;
OPTIMIZE TABLE t_nested FINAL;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;

-- Each merge recorded the pending mutation as applied (data version 3), so no mutation rewrites the merged part later.
SELECT 'merged parts', table, name FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_vertical', 't_horizontal', 't_chain', 't_nested') AND active
ORDER BY table;

SELECT 'statistics', table, column, estimates.min, estimates.max, estimates.null_count FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_vertical', 't_horizontal') AND active AND column IN ('c', 'n')
ORDER BY table, column;
SELECT 'statistics', table, column, estimates.min, estimates.max FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_chain' AND active AND column IN ('b', 'c')
ORDER BY column;
SELECT 'statistics', table, column, estimates.min, estimates.max FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_nested' AND active AND column = 'n.x';

SELECT 'pruned vertical', count() FROM t_vertical WHERE c = 42 SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT 'pruned vertical', count() FROM t_vertical WHERE n = 5 SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT 'pruned horizontal', count() FROM t_horizontal WHERE c = 42 SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT 'pruned horizontal', count() FROM t_horizontal WHERE n = 5 SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT 'pruned chain', count() FROM t_chain WHERE b < 1000 SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT 'pruned chain', count() FROM t_chain WHERE c >= 1000 SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT 'pruned nested', count() FROM t_nested WHERE `n.x` = 42 SETTINGS min_marks_per_part_to_load_statistics = 0;

SELECT 'values vertical', countIf(c = 42), countIf(n = 5) FROM t_vertical;
SELECT 'values horizontal', countIf(c = 42), countIf(n = 5) FROM t_horizontal;
SELECT 'values chain', min(b), max(b), min(c), max(c) FROM t_chain SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT 'values nested', countIf(`n.x` = 42) FROM t_nested;
SELECT 'aggregated vertical', min(c), max(c), sum(c) FROM t_vertical SETTINGS use_statistics_for_min_max_aggregation = 1, use_statistics_for_sum_avg_aggregation = 1;
SELECT 'aggregated horizontal', min(c), max(c), sum(c) FROM t_horizontal SETTINGS use_statistics_for_min_max_aggregation = 1, use_statistics_for_sum_avg_aggregation = 1;

DROP TABLE t_vertical;
DROP TABLE t_horizontal;
DROP TABLE t_chain;
DROP TABLE t_nested;
