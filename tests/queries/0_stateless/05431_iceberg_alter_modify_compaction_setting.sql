-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: requires `IcebergLocal` (USE_AVRO)
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- `ALTER TABLE ... MODIFY SETTING` and `RESET SETTING` change the settings of the background compaction stored in the
-- definition of an Iceberg table.

SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 1;
SET async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05431_iceberg_alter_modify_compaction_setting/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (id Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2, allow_experimental_iceberg_compaction = 0, iceberg_compaction_delay_bias = 100;

INSERT INTO t VALUES (1), (2);

SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';

ALTER TABLE t MODIFY SETTING iceberg_compaction_delay_bias = 600, iceberg_data_file_size_lower_threshold_compaction = 1000;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT id FROM t ORDER BY id;

ALTER TABLE t MODIFY SETTING allow_experimental_iceberg_compaction = 1;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
ALTER TABLE t MODIFY SETTING allow_experimental_iceberg_compaction = 0;

ALTER TABLE t RESET SETTING iceberg_compaction_delay_bias;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';

-- The writes and the reads work with the new settings.
INSERT INTO t VALUES (3);
SELECT id FROM t ORDER BY id;

-- Only the settings which are stored in the table definition and not in the Iceberg metadata can be changed.
ALTER TABLE t MODIFY SETTING iceberg_format_version = 1; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t MODIFY SETTING iceberg_metadata_file_path = 'metadata/v1.metadata.json'; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t RESET SETTING iceberg_format_version; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t MODIFY SETTING iceberg_compaction_delay_bias = 'abc'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED, CANNOT_PARSE_NUMBER, TYPE_MISMATCH, BAD_ARGUMENTS }
ALTER TABLE t ADD COLUMN x Int32, MODIFY SETTING iceberg_compaction_delay_bias = 600; -- { serverError NOT_IMPLEMENTED }
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';

-- The new settings are kept in the definition.
DETACH TABLE t;
ATTACH TABLE t;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT id FROM t ORDER BY id;

DROP TABLE t SYNC;
DROP TABLE iceberg_path;
