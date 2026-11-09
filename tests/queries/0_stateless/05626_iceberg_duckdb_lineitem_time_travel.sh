#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Time travel on a TPC-H lineitem table with two snapshots: 60175 rows inserted at 2025-05-02 12:21:19.060,
# then a rewrite to 51793 rows at 2025-05-02 12:21:20.005.
# The table and the expected results come from the DuckDB Iceberg extension tests,
# see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

# For metadata that has to be refused: prints `rejected` for an exception, but not for a raw Poco exception,
# a logical error, a missing file or a rejected metadata file name, which are not the reason to refuse it.
function rejected()
{
    local error
    error=$($CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -m1 'Code: ')
    if [ -z "$error" ]; then
        echo 'not rejected'
    elif grep -q -E 'Bad metadata file name|\((POCO_EXCEPTION|LOGICAL_ERROR|FILE_DOESNT_EXIST)\)' <<< "$error"; then
        echo 'unexpected error'
    else
        echo 'rejected'
    fi
}

# The location of the table is `./lineitem_iceberg`, and the paths of the manifest lists, manifests and data files
# are relative to `data/persistent/iceberg`, e.g. `lineitem_iceberg/metadata/snap-...avro`.
# Bug path-resolver: the paths are mangled to `tadata/snap-...avro`.
T="icebergLocal('data/persistent/iceberg/lineitem_iceberg')"
$CLICKHOUSE_LOCAL -q "
SELECT 'lineitem_iceberg';
SELECT count() FROM $T;
SELECT count() FROM $T SETTINGS iceberg_snapshot_id = 7817332053627255703;
SELECT count() FROM $T SETTINGS iceberg_snapshot_id = 2354745328521181395;
-- At the first snapshot, between the snapshots, after the last snapshot.
SELECT count() FROM $T SETTINGS iceberg_timestamp_ms = 1746188479060;
SELECT count() FROM $T SETTINGS iceberg_timestamp_ms = 1746188479061;
SELECT count() FROM $T SETTINGS iceberg_timestamp_ms = 1748736000000;
"

# Before the first snapshot.
echo "before the first snapshot $(rejected "SELECT count() FROM $T SETTINGS iceberg_timestamp_ms = 1746188479059")"
echo "2000-01-01 $(rejected "SELECT count() FROM $T SETTINGS iceberg_timestamp_ms = 946684800000")"
