#!/usr/bin/env bash
# A file that does not exist when its turn comes is skipped with `engine_file_skip_failed_data_files`,
# like any other file that cannot be opened.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

echo "1" > "$DATA_DIR/good1.csv"
echo "2" > "$DATA_DIR/good2.csv"

TABLE_FN="file(['$DATA_DIR/good1.csv', '$DATA_DIR/missing.csv', '$DATA_DIR/good2.csv'], CSV, 'a UInt32')"

# Without the setting: the missing file fails the query.
$CLICKHOUSE_CLIENT -q "SELECT sum(a) FROM $TABLE_FN" 2>&1 | grep -q -E "FILE_DOESNT_EXIST|CANNOT_STAT" && echo "1" || echo "0"

# With the setting enabled: the missing file is skipped, both existing files are read (sum=3).
# Every skipped file is logged with a warning, so silence server logs.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT sum(a) FROM $TABLE_FN SETTINGS engine_file_skip_failed_data_files = 1"
