#!/usr/bin/env bash
# `engine_file_skip_failed_data_files` skips broken input files, not healthy ones whose format cannot
# be created because of another file the query refers to. Such an error must propagate even if its
# code is one of a broken file, e.g. `FILE_DOESNT_EXIST`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=${CLICKHOUSE_USER_FILES_UNIQUE:?}

cleanup() {
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"

echo '1' > "$DATA_DIR/good.csv"

# The file to record the errors of the input format to, in a directory that does not exist.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/good.csv', CSV, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1,
        input_format_record_errors_file_path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_missing_dir/errors.csv'
" 2>&1 | grep -o -m1 "FILE_DOESNT_EXIST"

# The template of a row of the `Template` format, which does not exist.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/good.csv', Template, 'a UInt32')
    SETTINGS engine_file_skip_failed_data_files = 1,
        format_template_row = '${CLICKHOUSE_TEST_UNIQUE_NAME}_missing.template'
" 2>&1 | grep -o -m1 "FILE_DOESNT_EXIST"

# The healthy file is read.
$CLICKHOUSE_CLIENT -q "
    SELECT sum(a) FROM file('$DATA_DIR/good.csv', CSV, 'a UInt32') SETTINGS engine_file_skip_failed_data_files = 1"
