#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Format version 3 Iceberg tables with columns added with an `initial-default` value, so that the data files written
# before the column was added must be read with the default, written by Spark. From the Apache Impala test data,
# see `data_iceberg/impala/README.md`. The expected results are the ones of the Impala test `iceberg-v3-default-values.test`;
# for `binary` and `timestamptz` defaults, which Impala refuses to read, the values in the table metadata.
#
# Until https://github.com/ClickHouse/ClickHouse/issues/129992 is fixed, `initial-default` is not applied and NULL is returned.
# Reading the subcolumn `j.f2` of a struct column that is missing in an older data file fails with `NOT_FOUND_COLUMN_IN_BLOCK`:
# https://github.com/ClickHouse/ClickHouse/issues/128304

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- Column j with initial-default -1 is missing in the data file of the first row, with Parquet, ORC and Avro data files.
SELECT 'iceberg_v3_default_value';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_default_value') ORDER BY i;
SELECT i FROM icebergLocal('iceberg_v3/iceberg_v3_default_value') WHERE j = -1;
SELECT i FROM icebergLocal('iceberg_v3/iceberg_v3_default_value') WHERE j IS NULL;
SELECT 'iceberg_v3_default_value_orc';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_default_value_orc') ORDER BY i;
SELECT i FROM icebergLocal('iceberg_v3/iceberg_v3_default_value_orc') WHERE j = -1;
SELECT 'iceberg_v3_default_value_avro';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_default_value_avro') ORDER BY i;
SELECT i FROM icebergLocal('iceberg_v3/iceberg_v3_default_value_avro') WHERE j = -1;

-- Defaults of all primitive types.
SELECT 'iceberg_v3_all_types';
SELECT id, col_boolean, col_int, col_bigint, col_float, col_double, col_decimal, col_string, col_date, col_timestamp,
    hex(col_binary), col_timestamp_epoch, col_timestamp_iso
FROM icebergLocal('iceberg_v3/iceberg_v3_all_types') ORDER BY id;

-- A struct column without a default and a timestamptz column with a default added later.
SELECT 'test_complex_default';
SELECT i, j.f2, ts_tz FROM icebergLocal('iceberg_v3/test_complex_default') ORDER BY i;

-- The table was partitioned by identity(j) after j was added with initial-default -1.
SELECT 'test_default_part';
SELECT * FROM icebergLocal('iceberg_v3/test_default_part') ORDER BY i;
SELECT i FROM icebergLocal('iceberg_v3/test_default_part') WHERE j = -1 ORDER BY i;
SELECT i FROM icebergLocal('iceberg_v3/test_default_part') WHERE j = 5;
SELECT j, count() FROM icebergLocal('iceberg_v3/test_default_part') GROUP BY j ORDER BY j;
" 2>&1
