-- Tags: zookeeper, no-parallel, no-replicated-database, no-random-merge-tree-settings
-- Tag no-parallel: the failpoint is global and would fail the settings `ALTER`s of concurrent tests
-- Tag no-replicated-database: `ALTER`s of replicated and non-replicated types cannot be mixed in one query

-- A settings `ALTER` of `ReplicatedMergeTree` publishes the new live settings, then installs the metadata
-- recomputed from them (here: without `auto_minmax_index__block_number`), then commits the definition.
-- A failure between the first two steps must be rolled back as well: otherwise the live settings say
-- `enable_block_number_column = 0` while the metadata still declares the index over `_block_number`.

DROP TABLE IF EXISTS t_rollback_block_index SYNC;

CREATE TABLE t_rollback_block_index (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rollback_block_index', 'r1') ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_block_number_column = 1, enable_block_number_column = 1;

INSERT INTO t_rollback_block_index VALUES (1);

SYSTEM ENABLE FAILPOINT rmt_alter_settings_fail_before_metadata_install;

-- The settings-only path.
ALTER TABLE t_rollback_block_index MODIFY SETTING enable_block_number_column = 0; -- { serverError FAULT_INJECTED }

-- The mixed path, which writes a replicated entry.
ALTER TABLE t_rollback_block_index ADD COLUMN x UInt8, MODIFY SETTING enable_block_number_column = 0; -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT rmt_alter_settings_fail_before_metadata_install;

SELECT 'index', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_rollback_block_index' ORDER BY name;

SELECT 'setting kept', countSubstrings(create_table_query, 'enable_block_number_column = 1') FROM system.tables
WHERE database = currentDatabase() AND name = 't_rollback_block_index';

SELECT 'columns', groupArray(name) FROM system.columns
WHERE database = currentDatabase() AND table = 't_rollback_block_index';

-- The live settings still write `_block_number`, so a merge can build the index.
INSERT INTO t_rollback_block_index VALUES (2);
OPTIMIZE TABLE t_rollback_block_index FINAL;
SELECT 'merged', count(), sum(a), uniqExact(_block_number) FROM t_rollback_block_index;
SELECT 'merged part has the index', secondary_indices_marks_bytes > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_rollback_block_index' AND active;

-- Without the failpoint the same `ALTER` applies.
ALTER TABLE t_rollback_block_index MODIFY SETTING enable_block_number_column = 0;
SELECT 'after alter', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_rollback_block_index';

DROP TABLE t_rollback_block_index SYNC;
