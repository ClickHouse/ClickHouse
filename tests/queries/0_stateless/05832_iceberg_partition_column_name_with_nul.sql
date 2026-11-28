-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- The name of a partition column goes into the Avro schema of the manifest; a NUL byte in it aborted the server.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05832_iceberg_partition_column_name_with_nul/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (`\0` Int32) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet') PARTITION BY (`\0`);
INSERT INTO t SELECT -6 FROM numbers(793);
INSERT INTO t VALUES (1), (2);
SELECT `\0`, count() FROM t GROUP BY 1 ORDER BY 1;
SELECT count() FROM t WHERE `\0` = -6;
