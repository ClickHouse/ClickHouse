-- Tags: no-fasttest, no-replicated-database
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`
-- Requires `IcebergLocal` (USE_AVRO).

SET allow_insert_into_iceberg = 1;
SET allow_experimental_iceberg_compaction = 0;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 0;
SET async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05483_iceberg_metadata_log_limit/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE VIEW latest_metadata_log AS
SELECT
    toUInt32(extract(_file, '^v(\\d+)')) AS version,
    arrayMap(e -> toUInt32(extract(JSONExtractString(e, 'metadata-file'), '/v(\\d+)[^/]*$')), JSONExtractArrayRaw(json, 'metadata-log')) AS log
FROM file(concat(currentDatabase(), '/05483_iceberg_metadata_log_limit/metadata/*.metadata.json'), 'RawBLOB', 'json String')
ORDER BY version DESC
LIMIT 1;

CREATE TABLE t (id Int64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2, allow_experimental_iceberg_compaction = 0;

INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
INSERT INTO t VALUES (4);
INSERT INTO t VALUES (5);
SELECT 'default limit', * FROM latest_metadata_log;

-- The oldest entries leave the log first.
SET iceberg_metadata_previous_versions_max = 3;
INSERT INTO t VALUES (6);
SELECT 'limit 3', * FROM latest_metadata_log;
INSERT INTO t VALUES (7);
SELECT 'limit 3', * FROM latest_metadata_log;

SET iceberg_metadata_previous_versions_max = 1;
DELETE FROM t WHERE id = 1;
SELECT 'limit 1', * FROM latest_metadata_log;

SELECT groupArray(id) FROM (SELECT id FROM t ORDER BY id);
SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't';

DROP VIEW latest_metadata_log;
SET iceberg_delete_data_on_drop = 1;
DROP TABLE t SYNC;
DROP TABLE iceberg_path;
