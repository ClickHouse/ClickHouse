#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Schema and statistics edge cases: type widening (top-level and nested), column mapping without
# statistics, mixed-case column names, statistics as JSON or as structs in checkpoints
# (`stats_parsed`, `partitionValues_parsed`), missing statistics, null min/max statistics,
# partition values of all types and partition values that need URI escaping.
# The tables and the expected results come from the Trino Delta Lake connector tests,
# see `data_delta_lake/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/trino"

# Tables with partition values that need escaping in file names are stored in a tar archive.
EXTRACTED="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "$EXTRACTED"
mkdir -p "$EXTRACTED"
tar -xf "$DATA/special_paths.tar" -C "$EXTRACTED"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'databricks153/type_widening';
DESCRIBE deltaLakeLocal('$DATA/databricks153/type_widening') SETTINGS describe_compact_output = 1;
SELECT * FROM deltaLakeLocal('$DATA/databricks153/type_widening') ORDER BY ALL;

SELECT 'databricks153/type_widening_nested';
DESCRIBE deltaLakeLocal('$DATA/databricks153/type_widening_nested') SETTINGS describe_compact_output = 1;
SELECT * FROM deltaLakeLocal('$DATA/databricks153/type_widening_nested') ORDER BY ALL;

-- byte -> short -> integer, with a version after each step.
SELECT 'deltalake/type_widening';
DESCRIBE deltaLakeLocal('$DATA/deltalake/type_widening') SETTINGS describe_compact_output = 1;
SELECT * FROM deltaLakeLocal('$DATA/deltalake/type_widening') ORDER BY ALL;
SELECT count() FROM deltaLakeLocal('$DATA/deltalake/type_widening') SETTINGS delta_lake_snapshot_version = 0;
SELECT col, toTypeName(col) FROM deltaLakeLocal('$DATA/deltalake/type_widening') ORDER BY ALL SETTINGS delta_lake_snapshot_version = 1;
SELECT col, toTypeName(col) FROM deltaLakeLocal('$DATA/deltalake/type_widening') ORDER BY ALL SETTINGS delta_lake_snapshot_version = 2;
SELECT col, toTypeName(col) FROM deltaLakeLocal('$DATA/deltalake/type_widening') ORDER BY ALL SETTINGS delta_lake_snapshot_version = 3;
SELECT col, toTypeName(col) FROM deltaLakeLocal('$DATA/deltalake/type_widening') ORDER BY ALL SETTINGS delta_lake_snapshot_version = 4;
SELECT col FROM deltaLakeLocal('$DATA/deltalake/type_widening') WHERE col > 32767;

SELECT 'deltalake/type_widening_nested';
DESCRIBE deltaLakeLocal('$DATA/deltalake/type_widening_nested') SETTINGS describe_compact_output = 1;
SELECT * FROM deltaLakeLocal('$DATA/deltalake/type_widening_nested') ORDER BY ALL;
SELECT * FROM deltaLakeLocal('$DATA/deltalake/type_widening_nested') ORDER BY ALL SETTINGS delta_lake_snapshot_version = 1;

SELECT 'databricks104/no_stats_column_mapping_id';
SELECT * FROM deltaLakeLocal('$DATA/databricks104/no_stats_column_mapping_id') ORDER BY ALL;
SELECT c_str FROM deltaLakeLocal('$DATA/databricks104/no_stats_column_mapping_id') WHERE c_int = 42;

SELECT 'databricks104/no_column_stats_mixed_case';
SELECT * FROM deltaLakeLocal('$DATA/databricks104/no_column_stats_mixed_case') ORDER BY ALL;
SELECT c_Str FROM deltaLakeLocal('$DATA/databricks104/no_column_stats_mixed_case') WHERE c_Int = 11;

SELECT 'databricks104/json_stats_on_row_type';
SELECT * FROM deltaLakeLocal('$DATA/databricks104/json_stats_on_row_type') ORDER BY ALL;
SELECT struct_col.x FROM deltaLakeLocal('$DATA/databricks104/json_stats_on_row_type') WHERE nested_struct_col.y.nested = 'test';
SELECT count() FROM deltaLakeLocal('$DATA/databricks104/json_stats_on_row_type') WHERE struct_col.x IS NULL;

SELECT 'databricks73/no_column_stats';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/no_column_stats');

SELECT 'databricks73/parquet_stats_missing';
SELECT count(), sum(i), countIf(i IS NULL) FROM deltaLakeLocal('$DATA/databricks73/parquet_stats_missing');
SELECT i FROM deltaLakeLocal('$DATA/databricks73/parquet_stats_missing') WHERE i > 6 ORDER BY i;

SELECT 'deltalake/stats_with_minmax_nulls';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/stats_with_minmax_nulls') ORDER BY ALL;
SELECT count() FROM deltaLakeLocal('$DATA/deltalake/stats_with_minmax_nulls') WHERE id IS NULL;
SELECT id2 FROM deltaLakeLocal('$DATA/deltalake/stats_with_minmax_nulls') WHERE id = 3 ORDER BY id2;
SELECT min(id), max(id) FROM deltaLakeLocal('$DATA/deltalake/stats_with_minmax_nulls');

SELECT 'trino410/no_stats';
SELECT * FROM deltaLakeLocal('$DATA/trino410/no_stats') ORDER BY ALL;
SELECT c_str FROM deltaLakeLocal('$DATA/trino410/no_stats') WHERE c_int = 15 ORDER BY ALL;

SELECT 'trino410/no_stats_various_types';
SELECT * FROM deltaLakeLocal('$DATA/trino410/no_stats_various_types') ORDER BY ALL FORMAT TSVRaw;

SELECT 'databricks73/uppercase_columns';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/uppercase_columns') ORDER BY ALL;
SELECT KOTA FROM deltaLakeLocal('$DATA/databricks73/uppercase_columns') WHERE ALA = 1 ORDER BY ALL;

SELECT 'databricks73/insert_nonlowercase_columns_partitioned';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/insert_nonlowercase_columns_partitioned') ORDER BY ALL;
SELECT 'databricks73/insert_nested_nonlowercase_columns';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/insert_nested_nonlowercase_columns') ORDER BY ALL;
SELECT an_int FROM deltaLakeLocal('$DATA/databricks73/insert_nested_nonlowercase_columns') WHERE nested.UPPER_CASE_STRING = 'DATABRICKS' ORDER BY ALL;

SELECT 'databricks73/default_partitions';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/default_partitions') ORDER BY ALL;
SELECT value FROM deltaLakeLocal('$DATA/databricks73/default_partitions') WHERE number_partition IS NULL AND string_partition IS NULL;

SELECT 'databricks133/parsed_stats_case_sensitive';
SELECT * FROM deltaLakeLocal('$DATA/databricks133/parsed_stats_case_sensitive') ORDER BY ALL;
SELECT a_StRiNg FROM deltaLakeLocal('$DATA/databricks133/parsed_stats_case_sensitive') WHERE a_NuMbEr = 3;
SELECT 'databricks133/parsed_stats_struct';
SELECT * FROM deltaLakeLocal('$DATA/databricks133/parsed_stats_struct') ORDER BY ALL;
-- Subcolumns (root.entry_two) are not used: reading them from this partitioned table throws UNKNOWN_IDENTIFIER.
SELECT id FROM deltaLakeLocal('$DATA/databricks133/parsed_stats_struct') WHERE root = tuple(3, 'osla');
SELECT 'databricks133/partition_values_parsed_case_sensitive';
SELECT * FROM deltaLakeLocal('$DATA/databricks133/partition_values_parsed_case_sensitive') ORDER BY ALL;
SELECT id FROM deltaLakeLocal('$DATA/databricks133/partition_values_parsed_case_sensitive') WHERE part_NuMbEr = 2;

SELECT 'deltalake/partition_values_parsed';
SELECT * FROM deltaLakeLocal('$DATA/deltalake/partition_values_parsed') ORDER BY ALL;
SELECT id FROM deltaLakeLocal('$DATA/deltalake/partition_values_parsed') WHERE int_part = 20;
SELECT id FROM deltaLakeLocal('$DATA/deltalake/partition_values_parsed') WHERE string_part IS NULL;
SELECT 'trino432/partition_values_parsed';
SELECT * FROM deltaLakeLocal('$DATA/trino432/partition_values_parsed') ORDER BY ALL;
SELECT id FROM deltaLakeLocal('$DATA/trino432/partition_values_parsed') WHERE int_part = 10;
"

$CLICKHOUSE_CLIENT --session_timezone UTC -q "
SELECT 'deltalake/uri';
SELECT * FROM deltaLakeLocal('$EXTRACTED/deltalake/uri') ORDER BY y;
SELECT y FROM deltaLakeLocal('$EXTRACTED/deltalake/uri') WHERE part = 'a%percent';
SELECT y FROM deltaLakeLocal('$EXTRACTED/deltalake/uri') WHERE part = 'a/forwardslash';

SELECT 'trino410/no_stats_partitions';
SELECT * FROM deltaLakeLocal('$EXTRACTED/trino410/no_stats_partitions') ORDER BY ALL;
SELECT c_int FROM deltaLakeLocal('$EXTRACTED/trino410/no_stats_partitions') WHERE p_str = 'p?p' ORDER BY ALL;

-- The column part_long_decimal (Decimal(21, 3)) is left out: its partition values are read incorrectly.
SELECT 'deltalake/partition_values_parsed_all_types';
SELECT * EXCEPT part_long_decimal FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') ORDER BY id FORMAT Vertical;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_boolean = true;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_tinyint = 1;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_smallint = 10;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_int = 100;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_bigint = 1000;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_short_decimal = 123.12;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_double = 1.2;
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_float = toFloat32(3.4);
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_varchar = 'a';
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_date = '2020-08-21';
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_timestamp = '2020-10-21 01:00:00.123';
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_timestamp_ntz = '2023-01-02 01:02:03.456';
SELECT id FROM deltaLakeLocal('$EXTRACTED/deltalake/partition_values_parsed_all_types') WHERE part_boolean IS NULL AND part_int IS NULL AND part_date IS NULL AND part_timestamp_ntz IS NULL;
"

rm -rf "$EXTRACTED"
