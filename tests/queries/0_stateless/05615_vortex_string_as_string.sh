#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `output_format_vortex_string_as_string` (on by default) writes `String` as Vortex `Utf8` instead of
# `Binary`. Vortex does not check that `Utf8` values are valid UTF-8, so ClickHouse rejects the ones
# that are not.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
WORKING_DIR="${USER_FILES_PATH%/}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"

echo "Round trip with the default setting:"
$CLICKHOUSE_LOCAL -q "
    SELECT
        arrayJoin(['ascii', 'Привет', '😀', '']) AS s,
        if(s = '', NULL, s)::Nullable(String) AS ns,
        toLowCardinality(s) AS lc,
        [s, 'x'] AS arr,
        [s, NULL]::Array(Nullable(String)) AS arr_n,
        tuple(1, s)::Tuple(a UInt64, b String) AS t,
        unhex('FF00')::FixedString(2) AS f
    FORMAT Vortex" > "$WORKING_DIR/round_trip.vortex"
$CLICKHOUSE_LOCAL -q "SELECT * EXCEPT f, hex(f) FROM file('$WORKING_DIR/round_trip.vortex', Vortex) ORDER BY s"
$CLICKHOUSE_LOCAL -q "DESC file('$WORKING_DIR/round_trip.vortex', Vortex)"

# The same data written as `Utf8` and as `Binary`. A literal that is not valid UTF-8 cannot be a `Utf8`
# scalar, so it is pushed into the scan of the `Binary` file only: that tells the two types apart. The
# other conditions are pushed into both scans alike.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$WORKING_DIR/utf8.vortex', Vortex)
    SELECT concat('str', toString(number)) AS s, unhex(if(number % 2 = 0, 'FF00', '4142'))::FixedString(2) AS f
    FROM numbers(3000)
    SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$WORKING_DIR/binary.vortex', Vortex)
    SELECT concat('str', toString(number)) AS s, unhex(if(number % 2 = 0, 'FF00', '4142'))::FixedString(2) AS f
    FROM numbers(3000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_vortex_string_as_string = 0"

echo "The Utf8 file is smaller: $(( $(stat -c %s "$WORKING_DIR/utf8.vortex") < $(stat -c %s "$WORKING_DIR/binary.vortex") ))"

run_and_report_events() {
    local file=$1
    local condition=$2
    local query_id="${CLICKHOUSE_DATABASE}_vortex_string_$RANDOM$RANDOM"
    local result
    result=$($CLICKHOUSE_CLIENT --query_id="$query_id" -q "
        SELECT count(), sum(cityHash64(s)) FROM file('$WORKING_DIR/$file.vortex', Vortex)
        WHERE $condition
        SETTINGS input_format_vortex_filter_push_down = 1")
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    echo "$file, $condition: $result, pushed and dropped conjuncts: $($CLICKHOUSE_CLIENT -q "
        SELECT ProfileEvents['VortexFilterPushdownConjunctsPushed'], ProfileEvents['VortexFilterPushdownConjunctsDropped']
        FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()")"
}

for file in utf8 binary; do
    for condition in \
        "s = unhex('FF')" \
        "s = 'str42'" \
        "s IN ('str1', 'str2')" \
        "s LIKE 'str9%'" \
        "startsWith(s, 'str10')" \
        "f = unhex('FF00')"
    do
        run_and_report_events "$file" "$condition"
    done
done

check_rejected() {
    local label=$1
    local query=$2
    local errors
    errors=$($CLICKHOUSE_LOCAL -q "$query FORMAT Vortex" 2>&1 > /dev/null)
    if grep -q 'INCORRECT_DATA' <<< "$errors" && grep -q 'output_format_vortex_string_as_string' <<< "$errors"; then
        echo "$label: rejected"
    else
        echo "$label: not rejected"
    fi
}

echo "Values that are not valid UTF-8 are rejected:"
check_rejected "String" "SELECT unhex('FF') AS s"
check_rejected "Array(String)" "SELECT [unhex('FF')] AS s"
check_rejected "Tuple" "SELECT tuple(1, unhex('C3')) AS s"
check_rejected "LowCardinality(String)" "SELECT toLowCardinality(unhex('FF')) AS s"
check_rejected "Nullable(String)" "SELECT toNullable(unhex('FF')) AS s"
# Each value is invalid, while their concatenation (`é`, `€`) is valid.
check_rejected "Two rows of one character" "SELECT arrayJoin([unhex('C3'), unhex('A9')]) AS s"
check_rejected "Two rows of one character, three bytes" "SELECT arrayJoin([unhex('E282'), unhex('AC')]) AS s"
check_rejected "Two rows of one character with a NULL between" "SELECT arrayJoin([unhex('C3'), NULL, unhex('A9')]) AS s"
check_rejected "Two elements of one character" "SELECT [unhex('C3'), unhex('A9')] AS s"

errors=$($CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$WORKING_DIR/bad.vortex', Vortex) SELECT unhex('FF') AS s
    SETTINGS engine_file_truncate_on_insert = 1" 2>&1)
echo "INSERT INTO FUNCTION file: $(grep -q 'INCORRECT_DATA' <<< "$errors" && grep -q 'output_format_vortex_string_as_string' <<< "$errors" && echo rejected || echo not rejected)"

echo "Not rejected:"
$CLICKHOUSE_LOCAL -q "SELECT concat('a', unhex('C3A9')) AS s FORMAT Vortex" > "$WORKING_DIR/valid.vortex"
$CLICKHOUSE_LOCAL -q "SELECT s, hex(s) FROM file('$WORKING_DIR/valid.vortex', Vortex)"
# The bytes under a NULL are not written.
$CLICKHOUSE_LOCAL -q "SELECT hex(assumeNotNull(nullIf(unhex('FF'), unhex('FF'))))"
$CLICKHOUSE_LOCAL -q "
    SELECT nullIf(unhex('FF'), unhex('FF')) AS s, [nullIf(unhex('FF'), unhex('FF'))] AS arr
    FORMAT Vortex" > "$WORKING_DIR/null.vortex"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORKING_DIR/null.vortex', Vortex)"

echo "With output_format_vortex_string_as_string = 0 the same values are written as Binary:"
$CLICKHOUSE_LOCAL --output_format_vortex_string_as_string 0 -q "
    SELECT unhex('FF') AS s, [unhex('C3'), unhex('A9')] AS arr
    FORMAT Vortex" > "$WORKING_DIR/binary_values.vortex"
$CLICKHOUSE_LOCAL -q "SELECT hex(s), arrayMap(x -> hex(x), arr) FROM file('$WORKING_DIR/binary_values.vortex', Vortex)"

rm -rf "${WORKING_DIR}"
