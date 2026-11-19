#!/usr/bin/env bash
# `engine_file_skip_failed_data_files` skips files whose contents are structurally invalid for the
# format, not only files with unparsable values: a `Native` file with a corrupted type name (unknown or
# malformed), and a `JSONEachRow` file with a `NULL` for a non-`Nullable` column with
# `input_format_null_as_default = 0`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# One column `a` with one row. The `Native` block: the number of columns, the number of rows, the
# name of the column, its type and the value, a little-endian `UInt32`.
printf '\x01\x01\x01a\x06UInt32\x01\x00\x00\x00' > "$DATA_DIR/good.native"
printf '\x01\x01\x01a\x06XInt32\x02\x00\x00\x00' > "$DATA_DIR/broken.native"
printf '\x01\x01\x01a\x06Array(\x02\x00\x00\x00' > "$DATA_DIR/malformed.native"

echo '{"a":1}' > "$DATA_DIR/good.jsonl"
echo '{"a":null}' > "$DATA_DIR/null.jsonl"

NATIVE="file(['$DATA_DIR/good.native', '$DATA_DIR/broken.native'], Native, 'a UInt32')"
NATIVE_MALFORMED="file(['$DATA_DIR/good.native', '$DATA_DIR/malformed.native'], Native, 'a UInt32')"
JSON="file(['$DATA_DIR/good.jsonl', '$DATA_DIR/null.jsonl'], JSONEachRow, 'a UInt32')"

echo 'Native'
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM $NATIVE" 2>&1 | grep -o -m1 "UNKNOWN_TYPE"
# Every skipped file is logged with a warning, so silence server logs.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT sum(a) FROM $NATIVE SETTINGS engine_file_skip_failed_data_files = 1"
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM $NATIVE_MALFORMED" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT sum(a) FROM $NATIVE_MALFORMED SETTINGS engine_file_skip_failed_data_files = 1"

echo 'JSONEachRow'
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM $JSON SETTINGS input_format_null_as_default = 0" 2>&1 \
    | grep -o -m1 "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
    SELECT sum(a) FROM $JSON SETTINGS input_format_null_as_default = 0, engine_file_skip_failed_data_files = 1"
