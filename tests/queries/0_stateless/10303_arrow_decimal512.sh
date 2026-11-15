#!/usr/bin/env bash

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$CURDIR"/../shell_config.sh

export CLICKHOUSE_USER_FILES="$CURDIR/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$CLICKHOUSE_USER_FILES"
mkdir -p "$CLICKHOUSE_USER_FILES"

ARROW_FILE="10303_decimal512.arrow"

$CLICKHOUSE_CLIENT --query="INSERT INTO FUNCTION file('$ARROW_FILE', 'Arrow', 'd Decimal512(3)') SETTINGS engine_file_truncate_on_insert=1 SELECT toDecimal512('12345.678', 3)"

echo "-- arrow decimal512 roundtrip"
$CLICKHOUSE_CLIENT --query="SELECT toTypeName(d), d FROM file('$ARROW_FILE', 'Arrow', 'd Decimal512(3)')"

rm -rf "$CLICKHOUSE_USER_FILES"
