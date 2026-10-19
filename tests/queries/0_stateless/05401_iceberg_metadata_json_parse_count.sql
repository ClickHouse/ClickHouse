-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- Counts how many times `metadata.json` is parsed per SELECT from an Iceberg table.
-- The metadata files cache stores the raw string, so every `getMetadataJSONObject` call parses it again:
-- `getState`, `buildStorageMetadataFromState`, `getRelevantDataSnapshotFromTableStateSnapshot`,
-- and for a trivial `count` a second `getRelevantState` from `totalRows`.
-- Query shapes match q01-q03 of the CI-logs profiling report; the table settings match those tables.

SET allow_insert_into_iceberg = 1;
SET allow_experimental_iceberg_compaction = 0;
SET use_iceberg_metadata_files_cache = 1;
SET iceberg_delete_data_on_drop = 1;
SET async_insert = 0;
SET optimize_trivial_count_query = 1;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05401_iceberg_metadata_json_parse_count/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (event_date Date, id Int64, message String)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2, iceberg_use_version_hint = 1, allow_experimental_iceberg_compaction = 0;

INSERT INTO t VALUES ('2026-10-12', 1, 'a');
INSERT INTO t VALUES ('2026-10-13', 2, 'b');
INSERT INTO t VALUES ('2026-10-13', 3, 'c');
INSERT INTO t VALUES ('2026-10-14', 4, 'd');

-- The first run warms the metadata files cache, the second one is measured.
SELECT count() FROM t FORMAT Null;
SELECT count() FROM t FORMAT Null SETTINGS log_comment = 'q01_count';
SELECT count() FROM t WHERE event_date = '2026-10-13' FORMAT Null;
SELECT count() FROM t WHERE event_date = '2026-10-13' FORMAT Null SETTINGS log_comment = 'q02_count_where';
SELECT id, message FROM t WHERE event_date = '2026-10-13' AND id > 2 FORMAT Null;
SELECT id, message FROM t WHERE event_date = '2026-10-13' AND id > 2 FORMAT Null SETTINGS log_comment = 'q03_filter_scan';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['IcebergMetadataJSONParses'] AS metadata_json_parses,
    ProfileEvents['IcebergVersionHintUsed'] AS version_hint_reads,
    ProfileEvents['IcebergTrivialCountOptimizationApplied'] AS trivial_count
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'q0%'
ORDER BY log_comment;

DROP TABLE t SYNC;
DROP TABLE iceberg_path;
