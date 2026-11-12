-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: requires `IcebergLocal` (USE_AVRO)
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- The table setting `enable_cleanup_old_data_files_compaction` was named `allow_experimental_cleanup_old_data_files_compaction`
-- before. The table definitions written with the previous name still load, and both names can be modified.

SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 1;
SET async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05483_iceberg_cleanup_old_data_files_compaction_setting_alias/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

-- A definition with the previous name, as written by older versions.
CREATE TABLE t (id Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2, allow_experimental_cleanup_old_data_files_compaction = 1;

INSERT INTO t VALUES (1), (2);

DETACH TABLE t;
ATTACH TABLE t;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT id FROM t ORDER BY id;

ALTER TABLE t MODIFY SETTING allow_experimental_cleanup_old_data_files_compaction = 0;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
ALTER TABLE t RESET SETTING allow_experimental_cleanup_old_data_files_compaction;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';

ALTER TABLE t MODIFY SETTING allow_experimental_cleanup_old_data_files_compaction = 1;
-- The two names are one setting, so the definition keeps one entry for it.
ALTER TABLE t MODIFY SETTING enable_cleanup_old_data_files_compaction = 0;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
ALTER TABLE t MODIFY SETTING enable_cleanup_old_data_files_compaction = 1;
DETACH TABLE t;
ATTACH TABLE t;
SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT id FROM t ORDER BY id;

DROP TABLE t SYNC;
DROP TABLE iceberg_path;
