#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Timestamps, timestamp_ntz and dates in Delta tables written by Databricks, DuckDB and Trino:
# data values, partition values, file statistics in JSON and in checkpoints, nested timestamps.
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
-- The values -9999-12-31 23:59:59.999999 and -0001-01-01 00:00:00 (Trino results) are outside
-- of the range of DateTime64 and are left out.
SELECT 'databricks131/timestamp_ntz';
SELECT toTypeName(x) FROM deltaLakeLocal('$DATA/databricks131/timestamp_ntz') LIMIT 1;
SELECT x FROM deltaLakeLocal('$DATA/databricks131/timestamp_ntz')
WHERE x IS NULL OR x IN ('1582-10-05 00:00:00', '1582-10-14 23:59:59.999999', '2020-12-31 01:02:03.123456', '9999-12-31 23:59:59.999999')
ORDER BY x NULLS FIRST;
SELECT count() FROM deltaLakeLocal('$DATA/databricks131/timestamp_ntz');

SELECT 'duckdb155/timestamp_micros';
SELECT * FROM deltaLakeLocal('$DATA/duckdb155/timestamp_micros') ORDER BY id;
-- The file statistics have millisecond precision while the values have microsecond precision.
SELECT id FROM deltaLakeLocal('$DATA/duckdb155/timestamp_micros') WHERE ts > '2020-08-26 01:02:03.456';
SELECT id FROM deltaLakeLocal('$DATA/duckdb155/timestamp_micros') WHERE ts = '2020-08-26 01:02:03.456789';
SELECT id FROM deltaLakeLocal('$DATA/duckdb155/timestamp_micros') WHERE ts < '2020-08-26 01:02:03.456789';

SELECT 'duckdb155/timestamp_micros_checkpoint';
SELECT * FROM deltaLakeLocal('$DATA/duckdb155/timestamp_micros_checkpoint') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DATA/duckdb155/timestamp_micros_checkpoint') WHERE ts = '2020-08-26 01:02:03.456789';

SELECT 'duckdb155/timestamp_in_row_statistics';
SELECT * FROM deltaLakeLocal('$DATA/duckdb155/timestamp_in_row_statistics') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DATA/duckdb155/timestamp_in_row_statistics') WHERE r.ts = '2020-08-26 01:02:03.456789';

SELECT 'trino481/timestamp_tz_millis';
SELECT * FROM deltaLakeLocal('$DATA/trino481/timestamp_tz_millis') ORDER BY id;
SELECT id FROM deltaLakeLocal('$DATA/trino481/timestamp_tz_millis') WHERE ts = '2024-01-15 10:30:00.123';

SELECT 'databricks173/parquet_column_index_with_non_stats_column';
SELECT * FROM deltaLakeLocal('$DATA/databricks173/parquet_column_index_with_non_stats_column') ORDER BY x;
SELECT * FROM deltaLakeLocal('$DATA/databricks173/parquet_column_index_with_non_stats_column') WHERE x > 1 AND y IS NOT NULL;

SELECT 'databricks73/old_dates';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/old_dates') ORDER BY i;
SELECT i FROM deltaLakeLocal('$DATA/databricks73/old_dates') WHERE d < '1600-01-01' ORDER BY i;
-- Row 1 was written as 0100-01-01 and is read as 0099-12-30 due to the calendar shift (as in Trino),
-- while its file statistics say 0100-01-01. Filtering on it is not checked here, because the
-- statistics-based pruning drops the row.
SELECT i FROM deltaLakeLocal('$DATA/databricks73/old_dates') WHERE d = '1582-10-15';

SELECT 'databricks73/nested_timestamps';
SELECT * FROM deltaLakeLocal('$DATA/databricks73/nested_timestamps');
SELECT 'databricks73/nested_timestamps_parquet_stats';
SELECT count(), any(col1) FROM deltaLakeLocal('$DATA/databricks73/nested_timestamps_parquet_stats');
"

$CLICKHOUSE_CLIENT --session_timezone UTC -q "
-- Ids 2 and 3 hold -9999-12-31 23:59:59.999999 and -0001-01-01 00:00:00 (Trino results), which are
-- outside of the range of DateTime64.
SELECT 'databricks131/timestamp_ntz_partition';
SELECT id, part FROM deltaLakeLocal('$EXTRACTED/databricks131/timestamp_ntz_partition') WHERE id NOT IN (2, 3) ORDER BY id;
SELECT id FROM deltaLakeLocal('$EXTRACTED/databricks131/timestamp_ntz_partition') WHERE part = '2020-12-31 01:02:03.123456';
SELECT id FROM deltaLakeLocal('$EXTRACTED/databricks131/timestamp_ntz_partition') WHERE part IS NULL;

SELECT 'databricks192/timestamp_tz_partition';
SELECT id, part FROM deltaLakeLocal('$EXTRACTED/databricks192/timestamp_tz_partition') ORDER BY id;
SELECT id FROM deltaLakeLocal('$EXTRACTED/databricks192/timestamp_tz_partition') WHERE part = '2024-06-20 16:45:30.456';
SELECT id FROM deltaLakeLocal('$EXTRACTED/databricks192/timestamp_tz_partition') WHERE part > '2024-06-01 00:00:00' ORDER BY id;
SELECT id FROM deltaLakeLocal('$EXTRACTED/databricks192/timestamp_tz_partition') WHERE part IS NULL;
SELECT id, part FROM deltaLakeLocal('$EXTRACTED/databricks192/timestamp_tz_partition') ORDER BY id SETTINGS delta_lake_snapshot_version = 1;
SELECT id FROM deltaLakeLocal('$EXTRACTED/databricks192/timestamp_tz_partition') WHERE part = '2024-01-15 10:30:00.123' SETTINGS delta_lake_snapshot_version = 1;
"

rm -rf "$EXTRACTED"
