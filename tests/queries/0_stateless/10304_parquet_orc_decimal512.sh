#!/usr/bin/env bash

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$CURDIR"/../shell_config.sh

export CLICKHOUSE_USER_FILES="$CURDIR/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$CLICKHOUSE_USER_FILES"
mkdir -p "$CLICKHOUSE_USER_FILES"

PARQUET_FILE="10304_decimal512.parquet"
ORC_FILE="10304_decimal512.orc"

$CLICKHOUSE_CLIENT --query="INSERT INTO FUNCTION file('$PARQUET_FILE', 'Parquet', 'd Decimal512(3)') SETTINGS engine_file_truncate_on_insert=1 SELECT toDecimal512('123456789.987', 3)"

echo "-- parquet decimal512 roundtrip"
$CLICKHOUSE_CLIENT --query="SELECT toTypeName(d), d FROM file('$PARQUET_FILE', 'Parquet', 'd Decimal512(3)')"

$CLICKHOUSE_CLIENT --query="INSERT INTO FUNCTION file('$ORC_FILE', 'ORC', 'd Decimal512(3)') SETTINGS engine_file_truncate_on_insert=1 SELECT toDecimal512('-654.321', 3)"

echo "-- orc decimal512 roundtrip"
$CLICKHOUSE_CLIENT --query="SELECT toTypeName(d), d FROM file('$ORC_FILE', 'ORC', 'd Decimal512(3)')"

rm -rf "$CLICKHOUSE_USER_FILES"
