#!/usr/bin/env bash
# Tags: no-fasttest

# A message whose composite fields (`Array`, `Tuple`, `Map`, `Array(Tuple)`, `Nullable`) are
# read successfully, followed by a sibling field that fails to parse. The message-level rollback
# removes the partially read row from the composite columns, whose sub-columns are still held by
# the serializers of the nested fields. That rollback must not trip the exclusive-ownership
# assertion of `WrappedPtr` in debug and sanitizer builds, and the bad row must be skippable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMA="$CUR_DIR/format_schemas/05459_protobuf_rollback_composite_after_bad_sibling.proto:Row"
DATA_FILE="$CLICKHOUSE_TMP/05459_protobuf_rollback_composite_after_bad_sibling.bin"
trap 'rm -f "$DATA_FILE"' EXIT

COLUMNS="a Array(UInt32), t Tuple(x UInt32, s String), m Map(String, UInt32), n Array(Tuple(x UInt32, s String)), o Nullable(UInt32)"

# Write the data with `d` as a string, so that the second row has a value that is not a date.
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM values('$COLUMNS, d String',
        ([1, 2], (3, 'p'), map('k', 4), [(5, 'q')], 6, '2024-01-15'),
        ([7], (8, 'r'), map('l', 9), [(10, 's'), (11, 't')], 12, 'bad!'),
        ([13], (14, 'u'), map('m', 15), [(16, 'v')], 17, '2025-06-16'))
    INTO OUTFILE '$DATA_FILE' TRUNCATE
    FORMAT Protobuf SETTINGS format_schema = '$SCHEMA'"

echo "Without skipping errors:"
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM file('$DATA_FILE', 'Protobuf', '$COLUMNS, d Date')
    SETTINGS format_schema = '$SCHEMA'" 2>&1 | grep -o -m1 'CANNOT_PARSE_DATE'

echo "Skipping the bad row:"
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM file('$DATA_FILE', 'Protobuf', '$COLUMNS, d Date')
    SETTINGS format_schema = '$SCHEMA', input_format_allow_errors_num = 1"
