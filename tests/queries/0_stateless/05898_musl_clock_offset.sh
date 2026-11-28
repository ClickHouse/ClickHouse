#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The bundled musl shifts the clocks by `MUSL_CLOCK_REALTIME_OFFSET` and `MUSL_CLOCK_MONOTONIC_OFFSET` seconds.
# About 16 years ahead: past 2038-01-19, where a signed 32-bit `time_t` overflows.
OFFSET=500000000
# The test may run under a shifted clock itself.
unset MUSL_CLOCK_REALTIME_OFFSET MUSL_CLOCK_MONOTONIC_OFFSET

echo "now"
real=$($CLICKHOUSE_LOCAL -q "SELECT toUnixTimestamp(now())")
shifted=$(MUSL_CLOCK_REALTIME_OFFSET=$OFFSET $CLICKHOUSE_LOCAL -q "SELECT toUnixTimestamp(now())")
diff=$((shifted - real - OFFSET))
[ "$diff" -ge -60 ] && [ "$diff" -le 60 ] && echo "shifted" || echo "difference $((shifted - real))"
MUSL_CLOCK_REALTIME_OFFSET=$OFFSET $CLICKHOUSE_LOCAL -q "SELECT toYear(now64()) > 2038, toYear(today()) > 2038"

echo "file timestamps follow the clock"
file="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_clock_offset.txt"
echo "hello" > "$file"
MUSL_CLOCK_REALTIME_OFFSET=$OFFSET $CLICKHOUSE_LOCAL -q "SELECT line, abs(dateDiff('second', _time, now())) < 60 FROM file('$file', LineAsString)"
rm -f "$file"

echo "MergeTree with TTL, merges and mutations"
MUSL_CLOCK_REALTIME_OFFSET=$OFFSET $CLICKHOUSE_LOCAL -nq "
    CREATE TABLE t (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 1 DAY;
    INSERT INTO t SELECT now(), number FROM numbers(10);
    INSERT INTO t SELECT now() - INTERVAL 2 DAY, number FROM numbers(10);
    OPTIMIZE TABLE t FINAL;
    SELECT count(), uniqExact(x), min(d) > now() - INTERVAL 1 HOUR FROM t;
    ALTER TABLE t DELETE WHERE x < 5 SETTINGS mutations_sync = 1;
    SELECT count() FROM t;
"

echo "timed waits with a large monotonic offset"
MUSL_CLOCK_MONOTONIC_OFFSET=5000000000 MUSL_CLOCK_REALTIME_OFFSET=-$OFFSET timeout 60 $CLICKHOUSE_LOCAL -q "SELECT sleep(0.1)"
echo $?

echo "malformed value"
MUSL_CLOCK_REALTIME_OFFSET=1y $CLICKHOUSE_LOCAL -q "SELECT 1" 2>&1
echo $?
