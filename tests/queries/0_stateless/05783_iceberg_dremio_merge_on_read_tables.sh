#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The target tables of the `MERGE` tests of the Dremio test suite (`IcebergTestTables.MERGE_ON_READ_TARGET*`),
# see `data_iceberg/dremio/README.md`: a single data file of 100000 rows, referenced as
# `file:/tmp/iceberg-test-tables/v2/...`, while the table location is `/tmp/iceberg-test-tables/v2/...`.
# Expected values: an independent reader (PyIceberg).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/dremio"

q()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1"
}

echo v2/Merge_On_Read_Target_Partitioned
q "DESCRIBE icebergLocal('$DATA/v2/Merge_On_Read_Target_Partitioned') SETTINGS describe_compact_output = 1"
q "SELECT count(), sum(order_id), sum(source_id), round(sum(amount), 6), uniqExact(product_name) FROM icebergLocal('$DATA/v2/Merge_On_Read_Target_Partitioned')"
q "SELECT order_year, count(), sum(order_id), min(order_id), max(order_id), round(sum(amount), 6), min(order_date), max(order_date), uniqExact(product_name) FROM icebergLocal('$DATA/v2/Merge_On_Read_Target_Partitioned') GROUP BY order_year ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/v2/Merge_On_Read_Target_Partitioned') WHERE order_id % 10000 = 1234 ORDER BY ALL"
echo v2/Merge_On_Read_Target_Unpartitioned
q "DESCRIBE icebergLocal('$DATA/v2/Merge_On_Read_Target_Unpartitioned') SETTINGS describe_compact_output = 1"
q "SELECT count(), sum(order_id), sum(source_id), round(sum(amount), 6), uniqExact(product_name) FROM icebergLocal('$DATA/v2/Merge_On_Read_Target_Unpartitioned')"
q "SELECT order_year, count(), sum(order_id), min(order_id), max(order_id), round(sum(amount), 6), min(order_date), max(order_date), uniqExact(product_name) FROM icebergLocal('$DATA/v2/Merge_On_Read_Target_Unpartitioned') GROUP BY order_year ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/v2/Merge_On_Read_Target_Unpartitioned') WHERE order_id % 10000 = 1234 ORDER BY ALL"
