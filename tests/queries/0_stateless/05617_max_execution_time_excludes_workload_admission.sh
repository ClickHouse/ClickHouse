#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel: the `QUERY` resource is a singleton shared by the whole server

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
resource=r_$CLICKHOUSE_TEST_UNIQUE_NAME
holder=${CLICKHOUSE_TEST_UNIQUE_NAME}_holder
waiter=${CLICKHOUSE_TEST_UNIQUE_NAME}_waiter

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$holder' SYNC" >& /dev/null
    wait
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >& /dev/null
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS $resource" >& /dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE RESOURCE $resource (QUERY);
CREATE WORKLOAD $workload SETTINGS max_concurrent_queries = 1;
"

$CLICKHOUSE_CLIENT --query_id "$holder" --workload "$workload" --max_block_size 1 -q "SELECT sleepEachRow(1) FROM numbers(5) FORMAT Null" &
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$holder'") == 0 ]]; do
    sleep 0.1
done

# Waits for the only slot longer than `max_execution_time`, and should not fail
$CLICKHOUSE_CLIENT --query_id "$waiter" --workload "$workload" --max_execution_time 1 -q "SELECT count() FROM numbers(10)"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT query_duration_ms >= 1000
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND event_time >= now() - 600
    AND query_id = '$waiter'
    AND type = 'QueryFinish'
"
