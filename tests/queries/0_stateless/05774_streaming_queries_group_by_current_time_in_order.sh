#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A streaming query with `GROUP BY` the current time returns every group as soon as the time changes.
# The read of the `STREAM` has many streams, and most of them receive no data, so the aggregation must not wait for them.

SETTINGS="--enable_streaming_queries 1 --enable_analyzer 1 --enable_parallel_replicas 0 --automatic_parallel_replicas_mode 0 --optimize_aggregation_in_order_by_current_time 1"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_stream_by_time;
    CREATE TABLE t_stream_by_time (x UInt64) ENGINE = MergeTree ORDER BY tuple();
"

$CLICKHOUSE_CLIENT $SETTINGS -q "
    SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT nowInBlock() AS t, count() FROM t_stream_by_time STREAM GROUP BY t)
    WHERE explain ILIKE '%Flush on order key change%'
"

# The aggregation over an unbounded `STREAM` is allowed only if every group is emitted as soon as the time changes.
$CLICKHOUSE_CLIENT $SETTINGS -q "
    SELECT x, count() FROM t_stream_by_time STREAM GROUP BY x; -- { serverError ILLEGAL_STREAM }
    SELECT toSecond(nowInBlock()) AS t, count() FROM t_stream_by_time STREAM GROUP BY t; -- { serverError ILLEGAL_STREAM }
    SELECT count() FROM t_stream_by_time STREAM; -- { serverError ILLEGAL_STREAM }
    SELECT nowInBlock() AS t, count() FROM t_stream_by_time STREAM GROUP BY t WITH TOTALS; -- { serverError ILLEGAL_STREAM }
    SELECT nowInBlock() AS t, count() FROM t_stream_by_time STREAM GROUP BY t SETTINGS optimize_aggregation_in_order_by_current_time = 0; -- { serverError ILLEGAL_STREAM }
"

# Two groups are complete after the time changed twice: the query finishes while the rows are being inserted.
$CLICKHOUSE_CLIENT $SETTINGS --max_threads 8 --max_execution_time 300 -q "
    SELECT count() > 0 FROM t_stream_by_time STREAM GROUP BY nowInBlock() HAVING count() > 0 LIMIT 2
" &
query_pid=$!

for _ in {1..600}
do
    kill -0 $query_pid 2>/dev/null || break
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_stream_by_time SELECT number FROM numbers(10)"
    sleep 0.1
done

wait $query_pid

$CLICKHOUSE_CLIENT -q "DROP TABLE t_stream_by_time"
