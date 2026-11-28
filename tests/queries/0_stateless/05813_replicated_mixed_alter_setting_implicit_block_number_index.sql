-- Tags: zookeeper, no-replicated-database, no-random-merge-tree-settings
-- Tag no-replicated-database: `ALTER`s of replicated and non-replicated types cannot be mixed in one query

-- A mixed replicated `ALTER` (a schema change plus a setting) writes a replicated entry, and replaying it
-- regenerates the implicit indices from the policy of the running table. The local setting has to update
-- that policy too: otherwise disabling `enable_block_number_column` kept `auto_minmax_index__block_number`,
-- and the next merge built that index without reading `_block_number`, and enabling it did not add the index
-- until the table was reloaded.

DROP TABLE IF EXISTS t_mixed_block_index SYNC;

CREATE TABLE t_mixed_block_index (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mixed_block_index', 'r1') ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_block_number_column = 1, enable_block_number_column = 1;

INSERT INTO t_mixed_block_index VALUES (1);
INSERT INTO t_mixed_block_index VALUES (2);

SELECT 'before', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mixed_block_index' ORDER BY name;

ALTER TABLE t_mixed_block_index ADD COLUMN x UInt8, MODIFY SETTING enable_block_number_column = 0 SETTINGS alter_sync = 2;

SELECT 'live after disabling', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mixed_block_index';

OPTIMIZE TABLE t_mixed_block_index FINAL;
SELECT 'merged after disabling', count(), sum(a) FROM t_mixed_block_index;

ALTER TABLE t_mixed_block_index ADD COLUMN y UInt8, RESET SETTING enable_block_number_column SETTINGS alter_sync = 2;

SELECT 'live after reset', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mixed_block_index' ORDER BY name;

ALTER TABLE t_mixed_block_index ADD COLUMN z UInt8, MODIFY SETTING enable_block_number_column = 1 SETTINGS alter_sync = 2;

SELECT 'live after enabling', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mixed_block_index' ORDER BY name;

INSERT INTO t_mixed_block_index (a) VALUES (3);
OPTIMIZE TABLE t_mixed_block_index FINAL;

SELECT 'merged part has the index', secondary_indices_marks_bytes > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_mixed_block_index' AND active;

DETACH TABLE t_mixed_block_index;
ATTACH TABLE t_mixed_block_index;

SELECT 'after reattach', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_mixed_block_index' ORDER BY name;

DROP TABLE t_mixed_block_index SYNC;
