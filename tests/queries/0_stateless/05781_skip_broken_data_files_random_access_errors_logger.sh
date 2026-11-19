#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: uses the `Parquet` and `Protobuf` formats

# `engine_file_skip_failed_data_files` skips a file of a random-access format such as `Parquet` if the
# format cannot be created, because it reads the file then. The logger of `input_format_record_errors_file_path`
# is created with the format, but its errors are errors of the query: they must propagate instead of the
# healthy file being skipped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

$CLICKHOUSE_LOCAL -q "SELECT 1::UInt32 AS a FORMAT Parquet" > "$DATA_DIR/good.parquet"
echo 'broken' > "$DATA_DIR/broken.parquet"

# The errors are written in the `Protobuf` format, with a schema taken from a query, which fails.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/good.parquet', Parquet, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1, input_format_record_errors_file_path = '$DATA_DIR/errors.protobuf',
        errors_output_format = 'Protobuf', format_schema_source = 'query', format_schema_message_name = 'Message',
        format_schema = 'SELECT toString(toUInt32(materialize(char(120))))'
" 2>&1 | grep -o -m1 "CANNOT_PARSE_TEXT"

# The file of errors is in a directory that does not exist.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/good.parquet', Parquet, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1, input_format_record_errors_file_path = '$DATA_DIR/missing_dir/errors.csv'
" 2>&1 | grep -o -m1 "FILE_DOESNT_EXIST"

# With a valid file of errors, the broken file is still skipped. Every skipped file is logged with
# a warning, so silence server logs.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
    SELECT sum(a) FROM file('$DATA_DIR/{good,broken}.parquet', Parquet, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1, input_format_record_errors_file_path = '$DATA_DIR/errors.csv'"
