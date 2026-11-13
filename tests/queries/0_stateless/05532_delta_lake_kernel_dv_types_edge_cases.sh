#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Deletion vectors, type widening and other reader edge cases over the tables of delta-kernel-rs
# `kernel/tests/data` (see `data_delta_lake/kernel_data/README.md`). Expected values are those of
# `kernel/tests/integration/read.rs` and `kernel/tests/integration/write/remove_dv.rs`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data"

$CLICKHOUSE_LOCAL -q "
SET session_timezone = 'UTC';

SELECT '--- with-short-dv: an inline deletion vector removes rows 0, 1 and 2';
SELECT * FROM deltaLakeLocal('${DATA}/with-short-dv') ORDER BY id;

SELECT '--- table-with-dv-small: a deletion vector removes 2 of 10 rows';
SELECT arraySort(groupArray(value)) FROM deltaLakeLocal('${DATA}/table-with-dv-small');
SELECT count() FROM deltaLakeLocal('${DATA}/table-with-dv-small') WHERE value IN (0, 9);
SELECT '--- table-without-dv-small: the same table without the deletion vector';
SELECT arraySort(groupArray(value)) FROM deltaLakeLocal('${DATA}/table-without-dv-small');

SELECT '--- type-widening: columns read with their widened types';
DESCRIBE deltaLakeLocal('${DATA}/type-widening');
SELECT * FROM deltaLakeLocal('${DATA}/type-widening') ORDER BY byte_long FORMAT TSVWithNames;
SELECT '--- type-widening: filters on widened columns';
SELECT byte_long, int_long FROM deltaLakeLocal('${DATA}/type-widening') WHERE byte_long > 2147483647;
SELECT int_decimal FROM deltaLakeLocal('${DATA}/type-widening') WHERE int_decimal > 3;

SELECT '--- timestamp-partitioned-table: a timestamp partition value in ISO 8601 format';
DESCRIBE deltaLakeLocal('${DATA}/timestamp-partitioned-table');
SELECT * FROM deltaLakeLocal('${DATA}/timestamp-partitioned-table');
SELECT count() FROM deltaLakeLocal('${DATA}/timestamp-partitioned-table') WHERE time = '1971-07-22 03:06:40';

SELECT '--- compacted-log-files-table: a log with compacted commit files';
SELECT * FROM deltaLakeLocal('${DATA}/compacted-log-files-table') ORDER BY id;

SELECT '--- parquet_row_group_skipping: nested types';
SELECT * FROM deltaLakeLocal('${DATA}/parquet_row_group_skipping') ORDER BY chrono.date32 FORMAT TSVWithNames;
SELECT '--- parquet_row_group_skipping: a column of the table schema that is absent in the data files';
SELECT count(), countIf(missing IS NULL) FROM deltaLakeLocal('${DATA}/parquet_row_group_skipping');
SELECT count() FROM deltaLakeLocal('${DATA}/parquet_row_group_skipping') WHERE missing < 10;
SELECT count() FROM deltaLakeLocal('${DATA}/parquet_row_group_skipping') WHERE missing IS NULL;
"

echo '--- parquet_row_group_skipping: a filter on a column absent in the table schema'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('${DATA}/parquet_row_group_skipping') WHERE invalid < 10" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'

# The `variant` type is not supported: the table cannot be read, even without the variant column.
echo '--- unshredded-variant'
$CLICKHOUSE_LOCAL -q "SELECT id FROM deltaLakeLocal('${DATA}/unshredded-variant')" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
