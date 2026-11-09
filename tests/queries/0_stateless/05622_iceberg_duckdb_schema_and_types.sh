#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Column defaults, name mapping, column names, data files that do not match the table schema, and types.
# The tables and the expected results come from the DuckDB Iceberg extension tests,
# see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

# Prints the error code of a query that has to fail.
function error_code()
{
    $CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -o -m1 'Code: [0-9]*'
}

# Written by PyIceberg: 'col1' is the original column, all other columns were added later with an `initial-default`
# (two rows written before, one row after). Iceberg 'time' is read as Int64 microseconds since midnight.
# Bug initial-default: the defaults are not applied, NULL is returned.
DEFAULTS="icebergLocal('data/persistent/add_columns_with_defaults/default.db/add_columns_with_defaults')"
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'add_columns_with_defaults';
SELECT col1, col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_time, col_timestamp, col_timestamptz, col_string, col_uuid, hex(col_fixed), hex(col_binary)
FROM $DEFAULTS ORDER BY col1;
"

# The same with the columns added to a struct: one row written before the columns were added, one after.
STRUCT_DEFAULTS="icebergLocal('data/persistent/add_columns_with_defaults_in_struct/default.db/add_columns_with_defaults_in_struct')"
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'add_columns_with_defaults_in_struct';
SELECT x.col1, x.col_boolean, x.col_integer, x.col_long, x.col_float, x.col_double, x.col_decimal, x.col_date, x.col_time, x.col_timestamp, x.col_timestamptz, x.col_string, x.col_uuid, hex(x.col_fixed), hex(x.col_binary)
FROM (SELECT a AS x FROM $STRUCT_DEFAULTS) ORDER BY x.col_boolean;
"
# Reading a subcolumn of the struct directly. Bug https://github.com/ClickHouse/ClickHouse/issues/128304:
# fails with `NOT_FOUND_COLUMN_IN_BLOCK` for the data file
# whose struct has only 'col1', even for 'col1' itself.
$CLICKHOUSE_LOCAL -q "SELECT a.col1 FROM $STRUCT_DEFAULTS"

# Column names that differ only in case.
$CLICKHOUSE_LOCAL -q "
SELECT 'case_sensitive_names';
DESCRIBE icebergLocal('data/persistent/case_sensitive_names/default.db/case_sensitive_names') FORMAT TSV;
SELECT * FROM icebergLocal('data/persistent/case_sensitive_names/default.db/case_sensitive_names') ORDER BY ALL;
SELECT uSeR_Id FROM icebergLocal('data/persistent/case_sensitive_names/default.db/case_sensitive_names') ORDER BY ALL;
"

# A table whose data file (without field ids) was added from another directory, `warehouse/example/mock_data.parquet`.
# Bug path-resolver: the path of the data file is mangled to `ata/persistent/...`.
$CLICKHOUSE_LOCAL -q "
SELECT 'column_mapping';
SELECT * FROM icebergLocal('data/persistent/column_mapping/warehouse/default.db/my_table') ORDER BY ALL;
"

# The data files have no field ids, and the columns are mapped with `schema.name-mapping.default`.
# A column that is not in the name mapping is NULL. Time travel to the first snapshot.
# Only 'b' is checked, 'a' is a required column.
# v3: a and b are mapped; v3.1: b is removed from the name mapping; v3.2: the name mapping is empty;
# v4: b is removed from the schema, but not from the name mapping; v4.1: b is removed from both;
# v7: b is removed and added again with a different field id.
# Bug https://github.com/ClickHouse/ClickHouse/issues/130019: the name mapping is ignored, the columns removed from it are read by name.
echo 'name_mapping'
for v in 3 3.1 3.2 4 4.1 7
do
    snapshot=6597550917742534971
    [ "$v" = 7 ] && snapshot=2651609110244230974
    $CLICKHOUSE_LOCAL -q "
    SELECT '$v', countIf(b IS NULL), count()
    FROM icebergLocal('data/persistent/name_mapping/warehouse_1/mydb/t1', SETTINGS iceberg_metadata_file_path = 'metadata/v$v.metadata.json')
    SETTINGS iceberg_snapshot_id = $snapshot"
done

# A struct with a list of strings, where one data file has an unannotated group instead of a list.
# The columns that do not match the type in the schema can't be read, the other ones can.
ISSUE1270="icebergLocal('data/persistent/issue1270/warehouse/default.db/my_table', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
$CLICKHOUSE_LOCAL -q "
SELECT 'issue1270';
SELECT metadata.uid FROM $ISSUE1270 ORDER BY id;
SELECT id, metadata.uid FROM $ISSUE1270 WHERE id >= 3 ORDER BY id;
"
error_code "SELECT metadata.profiles FROM $ISSUE1270 FORMAT Null"
error_code "SELECT * FROM $ISSUE1270 FORMAT Null"
error_code "SELECT id FROM $ISSUE1270 WHERE notEmpty(metadata.profiles) FORMAT Null"

# One data file matches the schema, the other one has a different nested type for the column `payload`
# (a struct instead of a list, a map instead of a struct, ...).
# Bug https://github.com/ClickHouse/ClickHouse/issues/130020: the map of `map_to_struct` is read as a struct by the field id of
# its value, instead of refusing it.
echo 'mapping_failures'
for t in struct_to_list struct_to_map list_to_struct list_to_map map_to_struct map_to_list struct_child list_element map_value
do
    MF="icebergLocal('data/persistent/mapping_failures/$t', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')"
    echo "$t $(error_code "SELECT payload FROM $MF FORMAT Null") $($CLICKHOUSE_LOCAL -q "SELECT groupArray(id) FROM (SELECT id FROM $MF ORDER BY id)")"
done

# A map key, a map in a struct and a map in a list with a field id in the data file that the table never assigned.
# (DuckDB still reads the other columns of these data files, ClickHouse refuses the whole data file.)
echo 'missing_map_keys'
for t in map struct list
do
    echo "$t $(error_code "SELECT payload FROM icebergLocal('data/persistent/missing_map_keys/$t', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json') FORMAT Null")"
done
# A map column missing from the data file is empty.
# The data files are fine, but the maps in the manifest (column_sizes, value_counts and so on) have no field id for the key.
for t in missing_map manifest_column_sizes manifest_value_counts manifest_null_value_counts manifest_nan_value_counts manifest_lower_bounds manifest_upper_bounds
do
    echo "$t $($CLICKHOUSE_LOCAL -q "SELECT * FROM icebergLocal('data/persistent/missing_map_keys/$t', SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json')")"
done

$CLICKHOUSE_LOCAL -q "
SELECT 'uuid';
SELECT toTypeName(uuid) FROM icebergLocal('data/persistent/uuid') LIMIT 1;
SELECT uuid FROM icebergLocal('data/persistent/uuid') ORDER BY toString(uuid);
SELECT uuid FROM icebergLocal('data/persistent/uuid') WHERE uuid = '1571effb-facd-42a3-90e9-0af522e9b6c2';

-- An empty v3 table with a 'timestamptz_ns' column.
SELECT 'timestamptz_ns';
DESCRIBE icebergLocal('data/persistent/timestamptz_ns') FORMAT TSV;
SELECT count() FROM icebergLocal('data/persistent/timestamptz_ns');

-- decimal(38, 38) is the widest decimal type of Iceberg.
-- Bug metadata-file-names: \`decimal_max_precision.metadata.json\`.
SELECT 'invalid_decimal_type/decimal_max_precision';
DESCRIBE icebergLocal('data/persistent/invalid_decimal_type', SETTINGS iceberg_metadata_file_path = 'metadata/decimal_max_precision.metadata.json') FORMAT TSV;
"
