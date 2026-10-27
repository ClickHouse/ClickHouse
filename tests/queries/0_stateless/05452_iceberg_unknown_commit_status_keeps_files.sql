-- Tags: no-fasttest, no-parallel, no-replicated-database
-- Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).
-- Tag no-parallel: toggles a process-global failpoint, which another Iceberg insert could consume.
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- A commit with an unknown status may have published its metadata file, so the insert
-- must keep the data files and the manifests it references, also when the sink is destroyed.

SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 0;
SET async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05452_iceberg_unknown_commit_status_keeps_files/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (id Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;

INSERT INTO t VALUES (1);

SYSTEM ENABLE FAILPOINT iceberg_writes_unknown_commit_status;
INSERT INTO t VALUES (2); -- { serverError UNKNOWN_STATUS_OF_INSERT }
SYSTEM DISABLE FAILPOINT iceberg_writes_unknown_commit_status;

-- The metadata file of the second insert was published: its rows must be readable.
SELECT id FROM t ORDER BY id;

DROP TABLE t;
