#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ClickHouse converts a row to the type of a set key or a `FixedString` constant before comparing,
# and those conversions are not injective: `'a'` is padded to `'a\0'`, `UInt8` `2` becomes `true`,
# a `DateTime` is cut down to a `Date`. A filter pushed into the Vortex scan compares the raw values
# instead, so such conditions must not be pushed down: the results with the pushdown on and off
# must be the same.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
WORKING_DIR="${USER_FILES_PATH%/}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
DATA_FILE="${WORKING_DIR}/data.vortex"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', 'Vortex')
    SELECT 'a' AS s, 'a\0' AS z, toUInt8(2) AS u, toDateTime64('2020-01-01 12:00:00', 0, 'UTC') AS t
    SETTINGS engine_file_truncate_on_insert = 1"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE fixed_keys (k FixedString(2)) ENGINE = Memory;
    INSERT INTO fixed_keys VALUES ('a');
    CREATE TABLE bool_keys (k Bool) ENGINE = Memory;
    INSERT INTO bool_keys VALUES (true);
    CREATE TABLE date_keys (k Date) ENGINE = Memory;
    INSERT INTO date_keys VALUES ('2020-01-01');"

for push_down in 0 1; do
    echo "input_format_vortex_filter_push_down = $push_down"
    for condition in \
        "s IN (SELECT k FROM fixed_keys)" \
        "s NOT IN (SELECT k FROM fixed_keys)" \
        "u IN (SELECT k FROM bool_keys)" \
        "u NOT IN (SELECT k FROM bool_keys)" \
        "t IN (SELECT k FROM date_keys)" \
        "t NOT IN (SELECT k FROM date_keys)" \
        "s = toFixedString('a', 2)" \
        "s != toFixedString('a', 2)" \
        "s >= toFixedString('a', 2)" \
        "s < toFixedString('a', 2)" \
        "z = toFixedString('a', 1)" \
        "z <= toFixedString('a', 1)"
    do
        echo "$condition: $($CLICKHOUSE_CLIENT --input_format_vortex_filter_push_down $push_down -q "
            SELECT count() FROM file('$DATA_FILE', 'Vortex', 's String, z String, u UInt8, t DateTime64(0, \'UTC\')') WHERE $condition")"
    done
done

rm -rf "${WORKING_DIR}"
