#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `deltaLakeLocal` needs the delta-kernel-rs library, which fast test does not build.
# no-msan: delta-kernel-rs is Rust code that is not instrumented by MSan.

# Partitioning edge cases in Delta tables adopted from the delta-rs test suite
# (see data_delta_lake/delta_rs/README.md): NULL partition values, values that need URI escaping,
# numeric and typed partition columns, partition columns also stored in the data files,
# and a partition column renamed under column mapping.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/delta_rs"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- delta-0.8.0-null-partition: NULL is stored as __HIVE_DEFAULT_PARTITION__ in the path';
SELECT k, v FROM deltaLakeLocal('$DATA/delta-0.8.0-null-partition') ORDER BY v;
SELECT v FROM deltaLakeLocal('$DATA/delta-0.8.0-null-partition') WHERE k IS NULL;
SELECT v FROM deltaLakeLocal('$DATA/delta-0.8.0-null-partition') WHERE k = 'A';
SELECT count() FROM deltaLakeLocal('$DATA/delta-0.8.0-null-partition') WHERE k = '__HIVE_DEFAULT_PARTITION__';

SELECT '-- delta-0.8.0-special-partition: partition values with a slash and a space, escaped twice in the log';
SELECT x, y FROM deltaLakeLocal('$DATA/delta-0.8.0-special-partition') ORDER BY y;
SELECT y FROM deltaLakeLocal('$DATA/delta-0.8.0-special-partition') WHERE x = 'A/A';
SELECT y FROM deltaLakeLocal('$DATA/delta-0.8.0-special-partition') WHERE x = 'B B';

SELECT '-- delta-0.8.0-numeric-partition: two numeric partition columns';
DESCRIBE deltaLakeLocal('$DATA/delta-0.8.0-numeric-partition');
SELECT * FROM deltaLakeLocal('$DATA/delta-0.8.0-numeric-partition') ORDER BY ALL;
SELECT z FROM deltaLakeLocal('$DATA/delta-0.8.0-numeric-partition') WHERE x = 9 AND y = 9.9;

SELECT '-- delta-2.2.0-partitioned-types: integer and string partition columns';
DESCRIBE deltaLakeLocal('$DATA/delta-2.2.0-partitioned-types');
SELECT * FROM deltaLakeLocal('$DATA/delta-2.2.0-partitioned-types') ORDER BY ALL;
SELECT c3 FROM deltaLakeLocal('$DATA/delta-2.2.0-partitioned-types') WHERE c1 = 6;

SELECT '-- partition-type-primitives: partition columns of type string holding integer, float, boolean and date values';
DESCRIBE deltaLakeLocal('$DATA/partition-type-primitives');
SELECT * FROM deltaLakeLocal('$DATA/partition-type-primitives') ORDER BY id;
SELECT 'year = 2020', count(), groupUniqArray(year) FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE year = '2020';
SELECT 'year = 2021.0', count(), groupUniqArray(year) FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE year = '2021.0';
SELECT 'is_active', count() FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE is_active = 'true';
SELECT 'event_date', count() FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE event_date = '2023-01-01';
SELECT 'category', count() FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE category = 'A';
SELECT 'is_active IN', count() FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE is_active IN ('true', 'false');
SELECT 'year IN', count() FROM deltaLakeLocal('$DATA/partition-type-primitives') WHERE year IN ('2020', '2022.0');

SELECT '-- table_with_partitioning_mapping: partition column id renamed to newid under column mapping';
DESCRIBE deltaLakeLocal('$DATA/table_with_partitioning_mapping');
SELECT * FROM deltaLakeLocal('$DATA/table_with_partitioning_mapping') ORDER BY ALL;
SELECT description FROM deltaLakeLocal('$DATA/table_with_partitioning_mapping') WHERE newid = 2;

SELECT '-- table_with_column_mapping: partition column with spaces in its name under column mapping';
DESCRIBE deltaLakeLocal('$DATA/table_with_column_mapping');
SELECT * FROM deltaLakeLocal('$DATA/table_with_column_mapping') ORDER BY ALL;
SELECT count() FROM deltaLakeLocal('$DATA/table_with_column_mapping') WHERE \`Company Very Short\` = 'BMS';
"
