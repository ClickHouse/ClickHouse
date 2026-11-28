#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Filters on tables with null counts and bounds in the manifests, which can be used to skip data files.
# The tables and the expected results come from the DuckDB Iceberg extension tests,
# see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

NULL_STATS="icebergLocal('data/persistent/null_stats/default/test_nulls', SETTINGS iceberg_metadata_file_path = 'metadata/00003-9d6a621e-8a72-4190-a880-f6ca02e32b86.metadata.json')"
IS_NULL="icebergLocal('data/persistent/is_null_is_not_null')"
# The location is `file://data/persistent/...`.
MAP_STATS="icebergLocal('data/persistent/map_stats/default.db/map_stats', SETTINGS iceberg_metadata_file_path = 'metadata/00002-0e059d4b-e5e8-41af-a763-76cbf3a3d0bc.metadata.json')"
EXPRESSION="icebergLocal('data/persistent/expression_filter')"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
-- A boolean column with NULLs in several data files.
SELECT 'null_stats';
SELECT * FROM $NULL_STATS ORDER BY ALL;
SELECT * FROM $NULL_STATS WHERE flag IS NULL ORDER BY ALL;
SELECT * FROM $NULL_STATS WHERE flag IS NOT NULL ORDER BY ALL;

-- The column 'value' is all NULL in the data file with the ids 1 to 3.
SELECT 'is_null_is_not_null';
SELECT * FROM $IS_NULL ORDER BY id;
SELECT * FROM $IS_NULL WHERE value IS NULL ORDER BY id;
SELECT * FROM $IS_NULL WHERE value IS NOT NULL ORDER BY id;
SELECT * FROM $IS_NULL WHERE value = 'blah' ORDER BY id;
SELECT * FROM $IS_NULL WHERE isNotDistinctFrom(value, NULL) ORDER BY id;
SELECT * FROM $IS_NULL WHERE value IS DISTINCT FROM 'zzz' ORDER BY id;

-- Partitioned by day(timestamp), with bounds for the map key and value in the manifests.
SELECT 'map_stats';
SELECT * FROM $MAP_STATS ORDER BY ALL;
SELECT * FROM $MAP_STATS WHERE timestamp >= '2026-01-01' AND timestamp < '2026-01-02' AND metadata['key'] = 'value1' ORDER BY ALL;

-- IS NULL on a CASE expression must not skip data files.
SELECT 'expression_filter';
SELECT * FROM $EXPRESSION ORDER BY id;
SELECT id, CASE WHEN value = 'foo' THEN 'not null' ELSE NULL END AS role FROM $EXPRESSION WHERE role IS NULL ORDER BY id;
SELECT id, CASE WHEN value = 'foo' THEN 'not null' ELSE NULL END AS role FROM $EXPRESSION WHERE role IS NOT NULL ORDER BY id;

-- An empty table with the properties 'write.data.path' and 'write.metadata.path'.
SELECT 'custom_write_paths';
SELECT count() FROM icebergLocal('data/persistent/custom_write_paths');
"
