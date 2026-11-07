#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# Iceberg format version 2 tables with equality deletes (on the key column and on a non-key column) and a position delete,
# from the Dremio test suite, see `data_iceberg/dremio/README.md`. The table is partitioned by `category`, so
# an equality delete applies only to the data files of its partition with a lower sequence number.
# `products_with_eq_deletes_upper_case_columns` is the same table with upper case column names.
# The data files are referenced as `file:/tmp/iceberg-test-tables/...`, the delete files as `/tmp/iceberg-test-tables/...`.
# Expected values: `v2/products_with_eq_deletes/README.md` (450 rows remain of 1000) and the data and delete files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/dremio"

q()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1"
}

echo v2/products_with_eq_deletes
q "DESCRIBE icebergLocal('$DATA/v2/products_with_eq_deletes') SETTINGS describe_compact_output = 1"
q "SELECT category, count(), sum(product_id), min(product_id), max(product_id), countIf(color = 'green'), round(sum(weight), 6), sum(quantity), min(created_date), max(created_date) FROM icebergLocal('$DATA/v2/products_with_eq_deletes') GROUP BY category ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/v2/products_with_eq_deletes') WHERE product_id % 50 = 0 ORDER BY ALL"

# The rows deleted by the equality deletes on `product_id` and by the position delete are gone.
q "SELECT count() FROM icebergLocal('$DATA/v2/products_with_eq_deletes') WHERE product_id < 30 OR product_id BETWEEN 50 AND 52 OR product_id BETWEEN 100 AND 199"

# The equality delete on `color` applies only to the data files written before it.
q "SELECT count(), min(product_id), max(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes') WHERE color = 'green'"

# Every version of the table, see the steps in `products_with_eq_deletes/README.md`: 200, 170, 370, 333, 933, 453, 450 rows.
q "SELECT 2, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json')"
q "SELECT 3, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json')"
q "SELECT 4, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v4.metadata.json')"
q "SELECT 5, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v5.metadata.json')"
q "SELECT 6, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v6.metadata.json')"
q "SELECT 7, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v7.metadata.json')"
q "SELECT 8, count(), sum(product_id) FROM icebergLocal('$DATA/v2/products_with_eq_deletes', SETTINGS iceberg_metadata_file_path = 'metadata/v8.metadata.json')"
echo v2/products_with_eq_deletes_upper_case_columns
q "DESCRIBE icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns') SETTINGS describe_compact_output = 1"
q "SELECT category, count(), sum(PRODUCT_ID), min(PRODUCT_ID), max(PRODUCT_ID), countIf(color = 'green'), round(sum(weight), 6), sum(quantity), min(created_date), max(created_date) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns') GROUP BY category ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns') WHERE PRODUCT_ID % 50 = 0 ORDER BY ALL"

# The rows deleted by the equality deletes on `PRODUCT_ID` and by the position delete are gone.
q "SELECT count() FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns') WHERE PRODUCT_ID < 30 OR PRODUCT_ID BETWEEN 50 AND 52 OR PRODUCT_ID BETWEEN 100 AND 199"

# The equality delete on `color` applies only to the data files written before it.
q "SELECT count(), min(PRODUCT_ID), max(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns') WHERE color = 'green'"

# Every version of the table, see the steps in `products_with_eq_deletes/README.md`: 200, 170, 370, 333, 933, 453, 450 rows.
q "SELECT 2, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json')"
q "SELECT 3, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json')"
q "SELECT 4, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v4.metadata.json')"
q "SELECT 5, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v5.metadata.json')"
q "SELECT 6, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v6.metadata.json')"
q "SELECT 7, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v7.metadata.json')"
q "SELECT 8, count(), sum(PRODUCT_ID) FROM icebergLocal('$DATA/v2/products_with_eq_deletes_upper_case_columns', SETTINGS iceberg_metadata_file_path = 'metadata/v8.metadata.json')"
