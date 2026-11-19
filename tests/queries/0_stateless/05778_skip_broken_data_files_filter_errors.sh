#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

# With `engine_file_skip_failed_data_files`, an error of a condition of the query must propagate
# even if its code is one of a broken file. The `Parquet` reader can evaluate the conditions itself,
# so with the setting they are evaluated only after the read, and `PREWHERE` is not supported.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

# A healthy file whose value of `a` cannot be converted to a number.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$DATA_DIR/text.parquet') SELECT '1x' AS a, 'payload' AS payload"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$DATA_DIR/number.parquet') SELECT '5' AS a, 'five' AS payload"
head -c 200 /dev/zero | tr '\0' x > "$DATA_DIR/broken.parquet"

TEXT="file('$DATA_DIR/text.parquet', Parquet, 'a String, payload String')"

# The conversion error of the condition propagates instead of the file being skipped. The condition
# is neither moved to `PREWHERE` nor evaluated by the reader.
$CLICKHOUSE_CLIENT -q "
    SELECT payload FROM $TEXT WHERE toUInt32(a) > 0
    SETTINGS engine_file_skip_failed_data_files = 1, optimize_move_to_prewhere = 1
" 2>&1 | grep -o -m1 "CANNOT_PARSE_TEXT"

# An explicit `PREWHERE` is evaluated by the reader, so it is rejected with the setting...
$CLICKHOUSE_CLIENT -q "
    SELECT payload FROM $TEXT PREWHERE toUInt32(a) > 0
    SETTINGS engine_file_skip_failed_data_files = 1
" 2>&1 | grep -o -m1 "ILLEGAL_PREWHERE"
# ...and supported without it.
$CLICKHOUSE_CLIENT -q "SELECT payload FROM $TEXT PREWHERE toUInt32(a) > 0" 2>&1 | grep -o -m1 "CANNOT_PARSE_TEXT"

# The filter is still applied, and a broken file is still skipped.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
    SELECT payload FROM file('$DATA_DIR/{number,broken}.parquet', Parquet, 'a String, payload String')
    WHERE toUInt32(a) > 3
    SETTINGS engine_file_skip_failed_data_files = 1
"
