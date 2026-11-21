-- Tags: zookeeper, no-random-merge-tree-settings, no-shared-merge-tree
-- Tag no-random-merge-tree-settings: the test lists the implicit indices of a table.
-- Tag no-shared-merge-tree: the test covers the `ReplicatedMergeTree` metadata diff path.

-- Implicit indices are not stored in ZooKeeper, so every replicated `ALTER` regenerates them. The
-- implicit index of an unchanged expression alias must survive an unrelated `ALTER` on both replicas.
-- See `05408` for the opposite case of an alias whose index was deliberately dropped.

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_05410_r1 SYNC;
DROP TABLE IF EXISTS t_05410_r2 SYNC;

CREATE TABLE t_05410_r1 (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05410', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 8192;

CREATE TABLE t_05410_r2 (a UInt64, b UInt64 ALIAS a + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05410', 'r2') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1, index_granularity = 8192;

ALTER TABLE t_05410_r1 ADD COLUMN x UInt8 SETTINGS alter_sync = 2;
SELECT table, name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05410_r%' ORDER BY table, name;

INSERT INTO t_05410_r1 (a) SELECT number FROM numbers(100000);
SYSTEM SYNC REPLICA t_05410_r2;

SELECT 'issuer', count() FROM t_05410_r1 WHERE b = 1001 SETTINGS force_data_skipping_indices = 'auto_minmax_index_b';
SELECT 'follower', count() FROM t_05410_r2 WHERE b = 1001 SETTINGS force_data_skipping_indices = 'auto_minmax_index_b';

DROP TABLE t_05410_r1 SYNC;
DROP TABLE t_05410_r2 SYNC;
