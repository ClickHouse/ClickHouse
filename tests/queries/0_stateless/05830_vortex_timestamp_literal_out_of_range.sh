#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A pushed-down timestamp literal that Vortex cannot represent used to panic in the Rust library
# and abort the process. It is not pushed down now, and the query gives the same answer as without
# pushdown.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        toDateTime('2026-01-01 00:00:00', 'UTC') + number AS ts,
        toDateTime64('2026-01-01 00:00:00', 3, 'Europe/Berlin') + number AS ts_ms
    FROM numbers(10)
    FORMAT Vortex" > "$DATA_FILE"

for push_down in 1 0; do
    $CLICKHOUSE_LOCAL -q "
        SELECT
            (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE ts < fromUnixTimestamp64Second(700000000000)),
            (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE ts > fromUnixTimestamp64Second(-700000000000)),
            (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE ts_ms < fromUnixTimestamp64Milli(700000000000000, 'Europe/Berlin')),
            (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE ts >= '2026-01-01 00:00:05'),
            (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE ts_ms >= toDateTime64('2026-01-01 00:00:05', 3, 'Europe/Berlin'))
        SETTINGS input_format_vortex_filter_push_down = $push_down"
done

rm -f "$DATA_FILE"
