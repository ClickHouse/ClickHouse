#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg and Parquet support.

# Iceberg data types in tables from lithium-tech/iceberg-cxx, see `data_iceberg/iceberg_cxx/README.md`:
# all Trino types (expected values in `types/all_trino_types/create.sql`), all Spark types, decimals of
# precision 6 to 26, map and struct, and `initial-default` values of columns added by schema evolution.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local allows `icebergLocal` only for paths inside the current directory.
cd "$CUR_DIR/data_iceberg/iceberg_cxx" || exit 1

# Iceberg `time` is read as the number of microseconds since midnight: 45296123456 is 12:34:56.123456.
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'types/all_trino_types';
DESCRIBE icebergLocal('types/all_trino_types') SETTINGS describe_compact_output = 1;
SELECT * EXCEPT binary_col, hex(binary_col) FROM icebergLocal('types/all_trino_types');

-- Written by Spark, no generation script upstream; the values are cross-checked with DuckDB.
SELECT 'types/all_spark_types';
DESCRIBE icebergLocal('types/all_spark_types') SETTINGS describe_compact_output = 1;
SELECT * FROM icebergLocal('types/all_spark_types');

SELECT 'types/decimals';
DESCRIBE icebergLocal('types/decimals') SETTINGS describe_compact_output = 1;
SELECT * FROM icebergLocal('types/decimals');

SELECT 'types/unsupported_types';
DESCRIBE icebergLocal('types/unsupported_types') SETTINGS describe_compact_output = 1;
SELECT * FROM icebergLocal('types/unsupported_types');

-- Spark: a row with only id = 1, then 12 columns added by ALTER TABLE ADD COLUMN and a row with all of them.
SELECT 'streams/default_value';
DESCRIBE icebergLocal('streams/default_value') SETTINGS describe_compact_output = 1;
SELECT * EXCEPT binary_col, hex(binary_col) FROM icebergLocal('streams/default_value') ORDER BY id;
"

# `DefaultValues.json` is the same metadata with `initial-default` values for all added columns, so the first
# row must return them instead of NULL (upstream test `StreamsTest.EndToEndWithDefaultValue`).
# Bugs: the metadata file name is rejected (https://github.com/ClickHouse/ClickHouse/issues/129988) and
# `initial-default` is not applied (https://github.com/ClickHouse/ClickHouse/issues/129992).
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'streams/default_value with initial-default';
SELECT * EXCEPT binary_col, hex(binary_col) FROM icebergLocal('streams/default_value', SETTINGS iceberg_metadata_file_path = 'metadata/DefaultValues.json') ORDER BY id;
"
