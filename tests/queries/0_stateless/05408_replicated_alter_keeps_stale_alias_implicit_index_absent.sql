-- Tags: zookeeper, no-random-merge-tree-settings, no-shared-merge-tree
-- Tag no-random-merge-tree-settings: the test lists the implicit indices of a table.
-- Tag no-shared-merge-tree: the test covers the `ReplicatedMergeTree` metadata diff path.

-- Implicit indices are not stored in ZooKeeper, so every replicated `ALTER` regenerates them from the
-- columns and the table settings. A redefinition of an expression alias deliberately drops its
-- implicit index, because the index files of the existing parts hold values of the previous
-- definition. A later unrelated `ALTER` must not re-create that index: it would reuse the stale
-- files and prune rows that match the new definition. See `05112` for the redefinition itself.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_05408_r1 SYNC;
DROP TABLE IF EXISTS t_05408_r2 SYNC;

CREATE TABLE t_05408_r1 (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05408', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

CREATE TABLE t_05408_r2 (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05408', 'r2') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_05408_r1 SELECT number FROM numbers(100000);
SYSTEM SYNC REPLICA t_05408_r2;

SELECT 'redefined alias';
ALTER TABLE t_05408_r1 MODIFY COLUMN b UInt64 ALIAS a + 1000000 SETTINGS alter_sync = 2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05408_r%' ORDER BY table, name;

SELECT 'unrelated alter';
ALTER TABLE t_05408_r1 ADD COLUMN x UInt8 SETTINGS alter_sync = 2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05408_r%' ORDER BY table, name;

-- Reusing the index files of `a + 1` prunes the matching row away and returns `0` here.
SELECT 'issuer', count() FROM t_05408_r1 WHERE b = 1000001;
SELECT 'follower', count() FROM t_05408_r2 WHERE b = 1000001;

DROP TABLE t_05408_r1 SYNC;
DROP TABLE t_05408_r2 SYNC;
