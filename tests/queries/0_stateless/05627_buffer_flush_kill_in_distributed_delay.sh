#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# - no-replicated-database: the test stops distributed sends and relies on a single node

# Regression for #131024: an INSERT into a Buffer table flushes into a Distributed table that sits in the
# `bytes_to_delay_insert` delay. KILL QUERY must end that wait. The flush uses the shared Buffer context, which has
# no process list element, so `DistributedSink` has to take the query status from the thread of the INSERT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS buf_kill_delay;
DROP TABLE IF EXISTS dist_kill_delay;
DROP TABLE IF EXISTS data_kill_delay;
CREATE TABLE data_kill_delay (x UInt64) ENGINE = Null;
CREATE TABLE dist_kill_delay (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), data_kill_delay)
    SETTINGS bytes_to_delay_insert = 1, max_delay_to_insert = 3600;
-- The next INSERT into 'buf_kill_delay' flushes the buffer, because max_rows = 1 is already reached.
CREATE TABLE buf_kill_delay (x UInt64) ENGINE = Buffer(currentDatabase(), dist_kill_delay, 1, 100000, 100000, 1, 1, 1000000000, 1000000000);
SYSTEM STOP DISTRIBUTED SENDS dist_kill_delay;
-- Queue some bytes in 'dist_kill_delay', so that the next INSERT into it is delayed.
INSERT INTO dist_kill_delay SELECT number FROM numbers(1) SETTINGS prefer_localhost_replica = 0;
INSERT INTO buf_kill_delay VALUES (0);
"

QUERY_ID="buf_kill_delay_${CLICKHOUSE_DATABASE}_$$"

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" -q "INSERT INTO buf_kill_delay SETTINGS async_insert = 0 VALUES (1)" >/dev/null 2>&1 &
INSERT_PID=$!

# A one-row INSERT that still runs after a second is waiting in the delay.
started=0
for _ in {1..300}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND elapsed >= 1")" == "1" ]] && { started=1; break; }
    sleep 0.1
done
echo "insert is waiting in the delay: $started"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC FORMAT Null"

finished=0
for _ in {1..300}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'")" == "0" ]] && { finished=1; break; }
    sleep 0.1
done
echo "insert finished after KILL: $finished"

# Without the fix the INSERT sleeps for max_delay_to_insert, so release it before cleaning up.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH DISTRIBUTED dist_kill_delay"
wait "$INSERT_PID" 2>/dev/null

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS buf_kill_delay;
DROP TABLE IF EXISTS dist_kill_delay;
DROP TABLE IF EXISTS data_kill_delay;
"
