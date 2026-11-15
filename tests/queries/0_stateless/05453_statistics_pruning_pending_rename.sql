-- A pending `RENAME COLUMN` is applied to reads on the fly, while the part's statistics stay stored under the names
-- the columns have in the part. After `DROP COLUMN x, RENAME COLUMN y TO x`, the estimate found under `x` describes
-- the dropped column, so statistics-based part pruning must not use it while the mutation is pending.
-- https://github.com/ClickHouse/ClickHouse/issues/117323

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS t_05453;
CREATE TABLE t_05453 (x Int64 STATISTICS(basic), y Int64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();

-- Keep the mutation pending: mutations are executed by the merge scheduler.
SYSTEM STOP MERGES t_05453;
INSERT INTO t_05453 SETTINGS materialize_statistics_on_insert = 1 VALUES (1, 100), (2, 200);

ALTER TABLE t_05453 DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT 'the mutation is pending', count() > 0 FROM system.mutations
WHERE database = currentDatabase() AND table = 't_05453' AND NOT is_done;

SELECT 'the values a read sees', arraySort(groupArray(x)) FROM t_05453;

SELECT 'a renamed value', count() FROM t_05453 WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 1;
SELECT 'a renamed value, without pruning', count() FROM t_05453 WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 0;
SELECT 'a range of renamed values', count() FROM t_05453 WHERE x > 50 SETTINGS use_statistics_for_part_pruning = 1;
SELECT 'a value of the dropped column', count() FROM t_05453 WHERE x = 1 SETTINGS use_statistics_for_part_pruning = 1;

-- `min`/`max` answered from statistics must not use the dropped column's estimate either.
SELECT 'min and max', min(x), max(x) FROM t_05453 SETTINGS use_statistics_for_min_max_aggregation = 1;
SELECT 'min and max, without statistics', min(x), max(x) FROM t_05453 SETTINGS use_statistics_for_min_max_aggregation = 0;

-- Let the mutation materialize. Mutations are applied in order of their version, so waiting for a later one means
-- the rename is done too.
SYSTEM START MERGES t_05453;
ALTER TABLE t_05453 UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'after the mutation materialized', count() FROM t_05453 WHERE x = 100 SETTINGS use_statistics_for_part_pruning = 1;
SELECT 'after the mutation materialized, min and max', min(x), max(x) FROM t_05453 SETTINGS use_statistics_for_min_max_aggregation = 1;

DROP TABLE t_05453;
