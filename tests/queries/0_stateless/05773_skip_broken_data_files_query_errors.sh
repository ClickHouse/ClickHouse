#!/usr/bin/env bash
# `engine_file_skip_failed_data_files` skips only files that are broken. Here the files are healthy
# and the query fails for another reason, so the error must propagate even with the setting enabled
# instead of the file being skipped and the query returning an incomplete result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

echo '{"a":0}' > "$DATA_DIR/zero.jsonl"
echo '1' > "$DATA_DIR/one.csv"
echo '{"a":"1x"}' > "$DATA_DIR/text.jsonl"

# An exception in a default expression, evaluated while the file is being read.
$CLICKHOUSE_CLIENT -q "
    SELECT a, b FROM file('$DATA_DIR/zero.jsonl', JSONEachRow, 'a UInt32, b UInt32 DEFAULT intDiv(1, a)')
    SETTINGS engine_file_skip_failed_data_files = 1, input_format_defaults_for_omitted_fields = 1
" 2>&1 | grep -o -m1 "ILLEGAL_DIVISION"

# The same, with the error code of a broken file: `CANNOT_PARSE_TEXT` is raised by the conversion in the
# default expression, not by the parsing of the file, which is valid.
$CLICKHOUSE_CLIENT -q "
    SELECT a, b FROM file('$DATA_DIR/text.jsonl', JSONEachRow, 'a String, b UInt32 DEFAULT toUInt32(a)')
    SETTINGS engine_file_skip_failed_data_files = 1, input_format_defaults_for_omitted_fields = 1
" 2>&1 | grep -o -m1 "CANNOT_PARSE_TEXT"

# An invalid combination of settings, detected while the input format for the file is being created.
$CLICKHOUSE_CLIENT -q "
    SELECT * FROM file('$DATA_DIR/one.csv', CSV, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1, input_format_max_block_wait_ms = 1, input_format_connection_handling = 0
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
