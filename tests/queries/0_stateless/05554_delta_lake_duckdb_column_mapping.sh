#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `deltaLakeLocal` needs the delta-kernel-rs library, which fast test does not build.
# no-msan: delta-kernel-rs is Rust code that is not instrumented by MSan.

# Column mapping fixtures adopted from the duckdb-delta test suite (see data_delta_lake/duckdb/README.md
# and data_delta_lake/duckdb/column_mapping/README.upstream.md for what each fixture is about).
# The expected rows are the ones asserted by duckdb-delta in test/sql/inlined/column_mapping/*.test.
#
# Fixtures that are present in data_delta_lake/duckdb/column_mapping but not queried here,
# because ClickHouse reads them incorrectly:
# - `id_mode_logical_names` (expected `1 alice`, `2 bob`), `id_mode_nested_struct` (expected `(1,'alice')`, `(2,'bob')`)
#   and `id_mode_containers_nested_ids` (expected `1 [10,11] {'k':100}`, `2 [20] {'q':200}`): in `id` mode the reader
#   must resolve columns by Parquet field id, but ClickHouse resolves them by physical name and returns default
#   values for every column.
# - `id_mode_no_field_ids_logical`, `id_mode_no_field_ids_unmatched` and `invalid_nested_id_range`: the files cannot
#   be resolved in `id` mode, ClickHouse returns default values instead of an error.
# - `mode_without_feature` and `mode_without_feature_v3`: the `delta.columnMapping.mode` property without the
#   `columnMapping` feature must be ignored (expected `1 alice`, `2 bob`), but delta-kernel-rs refuses the
#   `physicalName` annotations.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/duckdb/column_mapping"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- id_mode_physical_names: Spark layout, physical names and field ids';
SELECT * FROM deltaLakeLocal('$DATA/id_mode_physical_names') ORDER BY a;

SELECT '-- id_mode_containers: Spark layout for a list and a map';
SELECT * FROM deltaLakeLocal('$DATA/id_mode_containers') ORDER BY a;
SELECT l, m['q'] FROM deltaLakeLocal('$DATA/id_mode_containers') WHERE a = 2;

SELECT '-- id_mode_missing_column: the first file predates column c';
SELECT * FROM deltaLakeLocal('$DATA/id_mode_missing_column') ORDER BY a;
SELECT a, c FROM deltaLakeLocal('$DATA/id_mode_missing_column') WHERE c IS NOT NULL;

SELECT '-- id_mode_no_field_ids_physical: no field ids, physical column names';
SELECT * FROM deltaLakeLocal('$DATA/id_mode_no_field_ids_physical') ORDER BY a;

SELECT '-- name_mode_physical_names';
SELECT * FROM deltaLakeLocal('$DATA/name_mode_physical_names') ORDER BY a;

SELECT '-- name_mode_legacy_protocol: reader version 2 implies column mapping';
SELECT * FROM deltaLakeLocal('$DATA/name_mode_legacy_protocol') ORDER BY a;

SELECT '-- name_mode_nested_ids: nested ids written by UniForm are ignored in name mode';
SELECT * FROM deltaLakeLocal('$DATA/name_mode_nested_ids') ORDER BY a;
SELECT arrayJoin(l), m['k'] FROM deltaLakeLocal('$DATA/name_mode_nested_ids') WHERE a = 1 ORDER BY 1;

SELECT '-- partitioned_two_columns_name_mode: partition columns before and between data columns, and a NULL partition';
DESCRIBE deltaLakeLocal('$DATA/partitioned_two_columns_name_mode');
SELECT * FROM deltaLakeLocal('$DATA/partitioned_two_columns_name_mode') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DATA/partitioned_two_columns_name_mode') WHERE p2 IS NULL;
SELECT id FROM deltaLakeLocal('$DATA/partitioned_two_columns_name_mode') WHERE p1 = 'z';

SELECT '-- spark_partitioned_name_mode';
SELECT * FROM deltaLakeLocal('$DATA/spark_partitioned_name_mode') ORDER BY id;
SELECT p, count() FROM deltaLakeLocal('$DATA/spark_partitioned_name_mode') GROUP BY p ORDER BY p;
SELECT id FROM deltaLakeLocal('$DATA/spark_partitioned_name_mode') WHERE p = 'b';
SELECT '-- spark_partitioned_id_mode';
SELECT * FROM deltaLakeLocal('$DATA/spark_partitioned_id_mode') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DATA/spark_partitioned_id_mode') WHERE p IS NULL;

SELECT '-- spark_renamed_name_mode: one row before RENAME COLUMN s TO who, one after';
SELECT * FROM deltaLakeLocal('$DATA/spark_renamed_name_mode') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DATA/spark_renamed_name_mode') WHERE who = 'before';
SELECT * FROM deltaLakeLocal('$DATA/spark_renamed_name_mode') SETTINGS delta_lake_snapshot_version = 1 FORMAT TSVWithNames;
SELECT '-- spark_renamed_id_mode';
SELECT * FROM deltaLakeLocal('$DATA/spark_renamed_id_mode') ORDER BY id;
SELECT * FROM deltaLakeLocal('$DATA/spark_renamed_id_mode') SETTINGS delta_lake_snapshot_version = 1 FORMAT TSVWithNames;

SELECT '-- spark_nested_name_mode: struct field s.x renamed to s.x2 between two inserts, structs inside an array and a map';
SELECT id, s, l, ls, m FROM deltaLakeLocal('$DATA/spark_nested_name_mode') ORDER BY id;
SELECT s.x2, s.y FROM deltaLakeLocal('$DATA/spark_nested_name_mode') ORDER BY id;
SELECT ls.a, mapValues(m).b FROM deltaLakeLocal('$DATA/spark_nested_name_mode') ORDER BY id;
SELECT '-- spark_nested_id_mode';
SELECT id, s, l, ls, m FROM deltaLakeLocal('$DATA/spark_nested_id_mode') ORDER BY id;
SELECT ls.a, mapValues(m).b FROM deltaLakeLocal('$DATA/spark_nested_id_mode') ORDER BY id;

SELECT '-- spark_quoted_name_mode: struct fields with a quote and a dot in their names';
DESCRIBE deltaLakeLocal('$DATA/spark_quoted_name_mode');
SELECT * FROM deltaLakeLocal('$DATA/spark_quoted_name_mode');
SELECT s.\`a\"b\`, s.\`c.d\` FROM deltaLakeLocal('$DATA/spark_quoted_name_mode');
SELECT '-- spark_quoted_id_mode';
SELECT * FROM deltaLakeLocal('$DATA/spark_quoted_id_mode');
"

echo "-- invalid annotations are refused"
for t in invalid_id_null invalid_id_string invalid_physical_name_empty; do
    echo "$t: $($CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DATA/$t')" 2>&1 | grep -o -m1 -E 'DELTA_KERNEL_ERROR|INCORRECT_DATA')"
done
