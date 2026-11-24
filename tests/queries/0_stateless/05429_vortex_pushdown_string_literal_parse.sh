#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A string constant compared with a temporal column is parsed with `cast_string_to_date_time_mode`.
# A literal pushed into the scan has to be parsed the same way: `best_effort` reads '20200101' as
# 2020-01-01, `basic` reads it as Unix seconds. The result must not depend on
# `input_format_vortex_filter_push_down`.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        toDateTime64('2020-01-01 00:00:00', 3, 'UTC') AS dt64,
        toDateTime('2020-01-01 00:00:00', 'UTC') AS dt
    UNION ALL
    SELECT
        toDateTime64(20200101, 3, 'UTC') AS dt64,
        toDateTime(20200101, 'UTC') AS dt
    FORMAT Vortex" > "$DATA_FILE"

for mode in best_effort basic; do
    for push_down in 0 1; do
        echo "mode = $mode, push_down = $push_down"
        for predicate in "dt64 = '20200101'" "dt = '20200101'" "dt64 >= '20200101'" "'20200101' < dt" "dt = '2020-01-01 00:00:00'"; do
            $CLICKHOUSE_LOCAL --param_predicate="$predicate" -q "
                SELECT {predicate:String}, count()
                FROM file('$DATA_FILE', 'Vortex')
                WHERE $predicate
                SETTINGS input_format_vortex_filter_push_down = $push_down, cast_string_to_date_time_mode = '$mode', session_timezone = 'UTC'
                FORMAT TSVRaw"
        done
    done
done

rm -f "$DATA_FILE"
