#!/usr/bin/env bash

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$CURDIR"/../shell_config.sh

export CLICKHOUSE_USER_FILES="$CURDIR/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$CLICKHOUSE_USER_FILES"
mkdir -p "$CLICKHOUSE_USER_FILES"

BSON_FILE="10305_decimal512.bson"

$CLICKHOUSE_CLIENT --query="INSERT INTO FUNCTION file('$BSON_FILE', 'BSONEachRow', 'd Decimal512(3)') SETTINGS engine_file_truncate_on_insert=1 SELECT toDecimal512('0.125', 3)"

echo "-- bson decimal512 roundtrip"
$CLICKHOUSE_CLIENT --query="SELECT toTypeName(d), d FROM file('$BSON_FILE', 'BSONEachRow', 'd Decimal512(3)')"

rm -rf "$CLICKHOUSE_USER_FILES"
