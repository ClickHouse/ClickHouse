-- Tags: zookeeper, no-random-merge-tree-settings, no-shared-merge-tree
-- Tag no-random-merge-tree-settings: the test lists the implicit indices of a table and needs wide parts.
-- Tag no-shared-merge-tree: the test covers the `ReplicatedMergeTree` metadata diff path.

-- Turning an expression alias into a physical column drops its implicit index, because the index files
-- of the existing parts hold values of the alias expression. Materializing the column rewrites only the
-- indices of the current metadata and keeps those files. Implicit indices are not stored in ZooKeeper,
-- so a later unrelated replicated `ALTER` regenerates them: it must not re-create the index of the now
-- physical column over the stale files. See `05408` for the same while the column stays an alias.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_05448_r1 SYNC;
DROP TABLE IF EXISTS t_05448_r2 SYNC;

CREATE TABLE t_05448_r1 (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05448', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '';

CREATE TABLE t_05448_r2 (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05448', 'r2') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0, auto_statistics_types = '';

INSERT INTO t_05448_r1 SELECT number FROM numbers(100000);
SYSTEM SYNC REPLICA t_05448_r2;

SELECT 'alias made physical';
ALTER TABLE t_05448_r1 MODIFY COLUMN b UInt64 DEFAULT 0 SETTINGS alter_sync = 2;
ALTER TABLE t_05448_r1 MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05448_r%' ORDER BY table, name;

SELECT 'unrelated alter';
ALTER TABLE t_05448_r1 ADD COLUMN x UInt8 SETTINGS alter_sync = 2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05448_r%' ORDER BY table, name;

-- Reusing the index files of `a + 1` prunes all rows away and returns `0` here.
SELECT 'issuer', count() FROM t_05448_r1 WHERE b = 0;
SELECT 'follower', count() FROM t_05448_r2 WHERE b = 0;
SELECT 'without skip indices', count() FROM t_05448_r1 WHERE b = 0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_05448_r1 SYNC;
DROP TABLE t_05448_r2 SYNC;
