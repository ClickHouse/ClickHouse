#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the CPU resource schedules the threads of every query on the server

# `CREATE OR REPLACE WORKLOAD` adds a limit to a workload that waits for CPU while its sibling of the same priority also waits

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RESOURCE="cpu_05876"
ALL="all_05876"
A="a_05876"
B="b_05876"

# Bound every client call, so a stuck query cannot block the cleanup
CALL_TIMEOUT=60

function cleanup()
{
    timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id IN ('${CLICKHOUSE_DATABASE}_$A', '${CLICKHOUSE_DATABASE}_$B') SYNC FORMAT Null" 2>/dev/null ||:
    wait
    timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $A" 2>/dev/null ||:
    timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $B" 2>/dev/null ||:
    timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $ALL" 2>/dev/null ||:
    timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS $RESOURCE" 2>/dev/null ||:
}

cleanup
trap cleanup EXIT TERM

$CLICKHOUSE_CLIENT -m -q "
    CREATE RESOURCE $RESOURCE (MASTER THREAD, WORKER THREAD);
    CREATE WORKLOAD $ALL SETTINGS max_concurrent_threads = 2;
    CREATE WORKLOAD $A IN $ALL;
    CREATE WORKLOAD $B IN $ALL;
"

# Each query wants more threads than `all` allows, so both workloads keep a pending request
for workload in $A $B
do
    timeout "$CALL_TIMEOUT" $CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_DATABASE}_$workload" --format Null \
        -q "SELECT count() FROM numbers_mt(1e13) SETTINGS workload = '$workload', max_threads = 8, max_execution_time = $CALL_TIMEOUT, max_rows_to_read = 0, max_bytes_to_read = 0" >/dev/null 2>&1 &
done

for _ in {1..300}
do
    waiting=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.scheduler WHERE resource = '$RESOURCE' AND type = 'request_queue' AND queue_length > 0")
    [ "$waiting" -eq 2 ] && break
    sleep 0.1
done
echo "waiting workloads: $waiting"

$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE WORKLOAD $A IN $ALL SETTINGS max_concurrent_threads = 100"

# The server is still alive
$CLICKHOUSE_CLIENT -q "SELECT 1"
