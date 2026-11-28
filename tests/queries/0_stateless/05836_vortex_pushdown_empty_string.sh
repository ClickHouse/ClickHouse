#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `optimize_empty_string_comparisons` rewrites `s = ''` and `s != ''` into `empty(s)` and
# `notEmpty(s)`, which have to reach the scan as well.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
DATA_FILE="${WORKING_DIR}/data.vortex"

# Three splits of 100 000 rows: the first two hold only empty strings.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', 'Vortex')
    SELECT
        if(number < 200000, '', toString(number)) AS s,
        if(number % 3 = 0, NULL, if(number % 3 = 1, '', 'x'))::Nullable(String) AS ns,
        if(number % 2 = 0, [], [number]) AS arr
    FROM numbers(300000)
    SETTINGS engine_file_truncate_on_insert = 1"

# Prints the count with and without the pushdown, the pushed and dropped conjuncts, and - for a
# condition that rules out the first 200 000 rows, which span at least two splits - whether whole
# splits were dropped.
run_and_report_events() {
    local query=$1
    local check_empty_splits=${2:-0}
    local query_id="${CLICKHOUSE_DATABASE}_vortex_empty_$RANDOM$RANDOM"
    echo "$query"
    $CLICKHOUSE_CLIENT --input_format_vortex_preserve_order 1 --query_id="$query_id" -q "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE $query"
    $CLICKHOUSE_CLIENT --input_format_vortex_filter_push_down 0 -q "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE $query"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT
            ProfileEvents['VortexFilterPushdownConjunctsPushed'],
            ProfileEvents['VortexFilterPushdownConjunctsDropped'],
            $check_empty_splits = 0 OR ProfileEvents['VortexScanEmptySplits'] >= 2
        FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
}

run_and_report_events "s != ''" 1
run_and_report_events "notEmpty(s)" 1
run_and_report_events "s = ''"
run_and_report_events "NOT empty(s)" 1
run_and_report_events "notEmpty(ns)"
run_and_report_events "empty(ns)"
run_and_report_events "NOT notEmpty(ns)"
# Not a string: left to ClickHouse.
run_and_report_events "notEmpty(arr)"

rm -rf "${WORKING_DIR}"
