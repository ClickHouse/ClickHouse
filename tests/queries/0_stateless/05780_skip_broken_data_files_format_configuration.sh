#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: uses the `Protobuf` format and the directory of format schemas

# With `engine_file_skip_failed_data_files`, a file whose data does not match the configuration of the
# format is skipped, but an error of the configuration itself propagates even if its code is one of a
# broken file: the input files are healthy then.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}
SCHEMA_DIR=${CLICKHOUSE_SCHEMA_FILES:?}
RESULTSET="${CLICKHOUSE_TEST_UNIQUE_NAME}_resultset.tpl"
ROW="${CLICKHOUSE_TEST_UNIQUE_NAME}_row.tpl"

cleanup() {
    rm -rf "$DATA_DIR"
    rm -f "$SCHEMA_DIR/$RESULTSET" "$SCHEMA_DIR/$ROW"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# The placeholders of the templates are not shell expansions.
# shellcheck disable=SC2016
printf 'HEADER\n${data}\n' > "$SCHEMA_DIR/$RESULTSET"
# shellcheck disable=SC2016
printf '${a:CSV}' > "$SCHEMA_DIR/$ROW"
printf 'HEADER\n1\n' > "$DATA_DIR/good.txt"
printf 'BROKEN\n2\n' > "$DATA_DIR/broken.txt"

echo 'Template'
TEMPLATE_SETTINGS="format_template_resultset = '$RESULTSET', format_template_row = '$ROW', format_template_rows_between_delimiter = '\n'"
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/{good,broken}.txt', Template, 'a UInt32') SETTINGS $TEMPLATE_SETTINGS
" 2>&1 | grep -o -m1 "INVALID_TEMPLATE_FORMAT"
# Every skipped file is logged with a warning, so silence server logs.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
    SELECT sum(a) FROM file('$DATA_DIR/{good,broken}.txt', Template, 'a UInt32')
    SETTINGS $TEMPLATE_SETTINGS, engine_file_skip_failed_data_files = 1"

# A schema taken from a query, which fails. The input file is never parsed.
echo 'Protobuf'
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/good.txt', Protobuf, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1, format_schema_source = 'query', format_schema_message_name = 'Message',
        format_schema = 'SELECT toString(toUInt32(materialize(char(120))))'
" 2>&1 | grep -o -m1 "CANNOT_PARSE_TEXT"
