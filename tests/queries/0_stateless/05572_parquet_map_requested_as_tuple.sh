#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# A Parquet map requested as a Tuple that does not name its repeated `key_value` group is a type mismatch.
# https://github.com/ClickHouse/ClickHouse/issues/130020

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the error code and the mismatched types of a query that has to fail.
function error()
{
    $CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -o -m1 -E 'Code: [0-9]+|parquet type is [^:]*' | paste -sd ' '
}

FILE="$(realpath "$CLICKHOUSE_TMP")/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
$CLICKHOUSE_LOCAL -q "SELECT 2::Int64 AS id, map('key', 'profile') AS payload, [map('k', 'v')] AS arr,
    map('a', map('b', 'c')) AS nested, CAST(tuple(map('k', 'v')), 'Tuple(m Map(String, String))') AS t FORMAT Parquet" > "$FILE"
F="file('$FILE', Parquet"

# The map at the top level, in a list, as a map value and in a struct.
error "SELECT * FROM $F, 'id Int64, payload Tuple(value Nullable(String))')"
error "SELECT * FROM $F, 'id Int64, payload Tuple(value Nullable(String))') SETTINGS input_format_parquet_allow_missing_columns = 0"
error "SELECT * FROM $F, 'id Int64, arr Array(Tuple(value String))')"
error "SELECT * FROM $F, 'id Int64, nested Map(String, Tuple(value String))')"
error "SELECT * FROM $F, 'id Int64, t Tuple(m Tuple(value String))')"

# A Tuple describing the schema tree of the map reads it literally.
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, payload Tuple(key_value Array(Tuple(key String, value String)))')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, payload Tuple(Array(Tuple(String, String)))')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, payload Tuple(key_value Array(Tuple(key String, value String)), x String)')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM $F, 'id Int64, payload Tuple(KEY_VALUE Array(Tuple(key String, value String)))')
    SETTINGS input_format_parquet_case_insensitive_column_matching = 1"
error "SELECT * FROM $F, 'id Int64, payload Tuple(KEY_VALUE Array(Tuple(key String, value String)))')
    SETTINGS input_format_parquet_case_insensitive_column_matching = 0"

# Other columns can still be read.
$CLICKHOUSE_LOCAL -q "SELECT id FROM $F, 'id Int64, payload Tuple(value Nullable(String))')"

rm -f "$FILE"

# An Iceberg table whose schema has `payload struct<value: string>`, with a data file that stores `payload` as a map.
# The table comes from the DuckDB Iceberg extension tests (test/sql/local/mapping_failure_variations.test), see data_iceberg/duckdb/README.md.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1
MF="icebergLocal('data/persistent/mapping_failures/map_to_struct', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
error "SELECT * FROM $MF ORDER BY ALL"
error "SELECT payload FROM $MF FORMAT Null"
$CLICKHOUSE_LOCAL -q "SELECT id FROM $MF ORDER BY id"
error "SELECT payload.value FROM $MF"
