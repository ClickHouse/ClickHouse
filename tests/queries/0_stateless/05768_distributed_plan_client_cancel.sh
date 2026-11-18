#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A distributed-plan query cancelled by its client must fail with the client's cancellation, not with the
# QUERY_WAS_CANCELLED that its in-memory exchanges report once the query's pipeline is cancelled.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number FROM numbers(100000)"

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}"
# 1000 rows x 0.001 s = 1 s per block, so the query runs for about 100 s.
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "
    SELECT x, count() FROM t WHERE NOT sleepEachRow(0.001) GROUP BY x FORMAT Null
    SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
        distributed_plan_default_reader_bucket_count = 3, distributed_plan_max_rows_to_broadcast = 0,
        max_rows_to_group_by = 0, max_execution_time = 300, max_block_size = 1000" >/dev/null 2>&1 &
client_pid=$!

# The tasks of the plan are queries under the initiator's id: cancel once one of them reads.
for _ in {1..600}; do
    reading=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE initial_query_id = '$query_id' AND query_id != '$query_id' AND read_rows > 0")
    [ "$reading" -ge 1 ] && break
    sleep 0.1
done
echo "reading: $((reading >= 1))"

kill -INT $client_pid
wait $client_pid

for _ in {1..600}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE initial_query_id = '$query_id'")" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT type, errorCodeToName(exception_code) FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type != 'QueryStart'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
