#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# Iceberg format version 2 tables with position delete files from the Dremio test suite,
# see `data_iceberg/dremio/README.md`. The tables are written to `/tmp/iceberg` and `/tmp/iceberg-test-tables/v2/...`.
# - `table_with_delete`: six partitions of 100 rows, six rows of the partition `qux` are deleted
#   (`v2_table_files_out.csv` of the Dremio test `TestIcebergTableFilesFunction`).
# - `v2/orders`: 600 orders over three years, without deletes.
# - `v2/multi_rowgroup_orders_with_deletes`: 9000 orders in files with several row groups, three snapshots with position deletes.
# Expected values: the Dremio tests and an independent reader (PyIceberg).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/dremio"

q()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1"
}

echo table_with_delete
q "DESCRIBE icebergLocal('$DATA/table_with_delete') SETTINGS describe_compact_output = 1"
q "SELECT category, count(), sum(id), min(id), max(id), min(date), max(date) FROM icebergLocal('$DATA/table_with_delete') GROUP BY category ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/table_with_delete') WHERE category = 'qux' AND id < 20 ORDER BY ALL"
q "SELECT category, count(), sum(id) FROM icebergLocal('$DATA/table_with_delete', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json') GROUP BY category ORDER BY ALL"
echo v2/orders
q "DESCRIBE icebergLocal('$DATA/v2/orders') SETTINGS describe_compact_output = 1"
q "SELECT order_year, count(), sum(order_id), min(order_id), max(order_id), round(sum(amount), 6), min(order_date), max(order_date), uniqExact(product_name) FROM icebergLocal('$DATA/v2/orders') GROUP BY order_year ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/v2/orders') ORDER BY ALL LIMIT 5"
q "SELECT order_year, count(), sum(order_id), min(order_id), max(order_id), round(sum(amount), 6), min(order_date), max(order_date), uniqExact(product_name) FROM icebergLocal('$DATA/v2/orders', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json') GROUP BY order_year ORDER BY ALL"
echo v2/multi_rowgroup_orders_with_deletes
q "DESCRIBE icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes') SETTINGS describe_compact_output = 1"
q "SELECT count(), sum(order_id), sum(source_id), round(sum(amount), 6) FROM icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes')"
q "SELECT order_year, count(), sum(order_id), min(order_id), max(order_id), round(sum(amount), 6), min(order_date), max(order_date), uniqExact(product_name) FROM icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes') GROUP BY order_year ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes') WHERE order_id % 1000 = 7 ORDER BY ALL"
q "SELECT count(), sum(order_id), round(sum(amount), 6) FROM icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json')"
q "SELECT count(), sum(order_id), round(sum(amount), 6) FROM icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json')"
q "SELECT count(), sum(order_id), round(sum(amount), 6) FROM icebergLocal('$DATA/v2/multi_rowgroup_orders_with_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v4.metadata.json')"
