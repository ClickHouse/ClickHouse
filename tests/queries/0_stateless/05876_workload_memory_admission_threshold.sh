#!/usr/bin/env bash
# Tags: no-parallel
# The memory reservation resource is shared with the other workload tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
query_id=q_$CLICKHOUSE_TEST_UNIQUE_NAME

resource=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.resources WHERE unit = 'MemoryByte'")
created_resource=
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null SETTINGS kill_throw_if_noop = false" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >/dev/null 2>&1 || :
    if [[ -n $created_resource ]]; then
        $CLICKHOUSE_CLIENT -q "DROP RESOURCE $resource" >/dev/null 2>&1 || :
    fi
}
trap cleanup EXIT

if [[ -z $resource ]]; then
    resource=memory_$CLICKHOUSE_TEST_UNIQUE_NAME
    $CLICKHOUSE_CLIENT -q "CREATE RESOURCE $resource (MEMORY RESERVATION)"
    created_resource=1
fi
$CLICKHOUSE_CLIENT -q "CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_queue = '1Mi'"

$CLICKHOUSE_CLIENT -q "SELECT admission_limit FROM system.scheduler WHERE resource = '$resource' AND path LIKE '%/$workload/limit'"

# Below the threshold a query with zero reservation runs
$CLICKHOUSE_CLIENT --workload "$workload" --workload_admission_timeout_ms 60000 -q "SELECT 'admitted below the threshold'"

# The reservation keeps the workload above the threshold while the query runs
$CLICKHOUSE_CLIENT --query_id "$query_id" --workload "$workload" --reserve_memory 10485760 --max_block_size 1 \
    -q "SELECT sleepEachRow(1) FROM numbers(300) FORMAT Null" >/dev/null 2>&1 &

until [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'") == 1 ]]; do
    sleep 0.1
done

# Above the threshold a new query waits, even with zero reservation, until the admission timeout
$CLICKHOUSE_CLIENT --workload "$workload" --workload_admission_timeout_ms 100 -q "SELECT 'unexpected'" 2>&1 \
    | grep -o -m1 "MEMORY_RESERVATION_ACQUISITION_TIMEOUT"

# A waiting query starts once the memory is released
$CLICKHOUSE_CLIENT --workload "$workload" --workload_admission_timeout_ms 60000 -q "SELECT 'admitted after release'" &
waiting_pid=$!
until [[ $($CLICKHOUSE_CLIENT -q "SELECT sum(pending) FROM system.scheduler WHERE resource = '$resource' AND path LIKE '%/$workload/%'") == 1 ]]; do
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait $waiting_pid
wait
