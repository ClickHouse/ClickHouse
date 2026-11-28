#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Set::execute` converts the column to the type of the set keys, while an `IN` pushed into the
# Vortex scan would convert the keys to the type of the column. Between a string and a number the
# two disagree (`'01'` parses to `1`, but `1` is looked up as `'1'`), so such sets must not be
# pushed down: the results with the pushdown on and off must be the same.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
WORKING_DIR="${USER_FILES_PATH%/}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
DATA_FILE="${WORKING_DIR}/data.vortex"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', 'Vortex')
    SELECT toUInt64(1) AS u, toFloat64(1) AS f, '01' AS s
    SETTINGS engine_file_truncate_on_insert = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE string_keys (k String) ENGINE = Memory;
    INSERT INTO string_keys VALUES ('01');
    CREATE TABLE number_keys (k UInt64) ENGINE = Memory;
    INSERT INTO number_keys VALUES (1);"

for push_down in 0 1; do
    echo "input_format_vortex_filter_push_down = $push_down"
    for condition in \
        "u IN (SELECT k FROM string_keys)" \
        "u NOT IN (SELECT k FROM string_keys)" \
        "f IN (SELECT k FROM string_keys)" \
        "f NOT IN (SELECT k FROM string_keys)" \
        "s IN (SELECT k FROM number_keys)" \
        "s NOT IN (SELECT k FROM number_keys)"
    do
        echo "$condition: $($CLICKHOUSE_CLIENT --input_format_vortex_filter_push_down $push_down -q "
            SELECT count() FROM file('$DATA_FILE', 'Vortex', 'u UInt64, f Float64, s String') WHERE $condition")"
    done
done

rm -rf "${WORKING_DIR}"
