#!/usr/bin/env bash
# Tags: replica, zookeeper, no-shared-merge-tree, no-replicated-database
# no-replicated-database: creates two explicit replicas (r1, r2) sharing one ZooKeeper path

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TIMEOUT_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_timeout"
OPTIMIZE_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_optimize"
SYNC_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_sync"
TIMEOUT_OUT="${CLICKHOUSE_TMP}/${TIMEOUT_ID}.out"
OPTIMIZE_OUT="${CLICKHOUSE_TMP}/${OPTIMIZE_ID}.out"
SYNC_OUT="${CLICKHOUSE_TMP}/${SYNC_ID}.out"
trap 'rm -f "$TIMEOUT_OUT" "$OPTIMIZE_OUT" "$SYNC_OUT"' EXIT

# r2 never executes its replication queue, so the fetch of the second part stays queued and every
# wait for r2's queue lasts until it is cancelled or times out.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE r1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05498/t', 'r1') ORDER BY k;
    CREATE TABLE r2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05498/t', 'r2') ORDER BY k;
    INSERT INTO r1 VALUES (1);
    SYSTEM SYNC REPLICA r2;
    SYSTEM STOP REPLICATION QUEUES r2;
    INSERT INTO r1 VALUES (2);
    SYSTEM SYNC REPLICA r2 PULL;
    SELECT count() > 0 FROM system.replication_queue WHERE database = currentDatabase() AND table = 'r2';
"

# receive_timeout = 120 bounds every wait below, so a wait that ignores cancellation gives a wrong
# result instead of hanging the test.

# 1. max_execution_time stops OPTIMIZE waiting for the queue between merge selection retries
#    (r2 has a single active part, so the selection fails and is retried). Code: 159 is also
#    reported once all retries are done, so the query must be gone well before that.
$CLICKHOUSE_CLIENT --query_id "$TIMEOUT_ID" -q "
    OPTIMIZE TABLE r2 PARTITION tuple()
    SETTINGS max_execution_time = 2, timeout_overflow_mode = 'throw', receive_timeout = 120
" > "$TIMEOUT_OUT" 2>&1 &
wait_for_query_to_start "$TIMEOUT_ID" 120 $!
wait_for_queries_to_finish 60
wait
grep -o -m1 'Code: 159' "$TIMEOUT_OUT"

# 2. KILL QUERY stops the same wait.
$CLICKHOUSE_CLIENT --query_id "$OPTIMIZE_ID" -q "
    OPTIMIZE TABLE r2 PARTITION tuple() SETTINGS receive_timeout = 120
" > "$OPTIMIZE_OUT" 2>&1 &
wait_for_query_to_start "$OPTIMIZE_ID"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$OPTIMIZE_ID' ASYNC FORMAT Null"
wait_for_queries_to_finish 60
wait
grep -o -m1 'Code: 394' "$OPTIMIZE_OUT"

# 3. KILL QUERY stops SYSTEM SYNC REPLICA.
$CLICKHOUSE_CLIENT --receive_timeout 120 --query_id "$SYNC_ID" -q "SYSTEM SYNC REPLICA r2" > "$SYNC_OUT" 2>&1 &
wait_for_query_to_start "$SYNC_ID"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$SYNC_ID' ASYNC FORMAT Null"
wait_for_queries_to_finish 60
wait
grep -o -m1 'Code: 394' "$SYNC_OUT"

# 4. With timeout_overflow_mode = 'break', max_execution_time also stops SYSTEM SYNC REPLICA, and the
#    error is the max_execution_time one, not "command timed out. See the 'receive_timeout' setting".
$CLICKHOUSE_CLIENT --receive_timeout 120 --max_execution_time 2 --timeout_overflow_mode break \
    -q "SYSTEM SYNC REPLICA r2" 2>&1 | grep -o -m1 'Timeout exceeded: elapsed'

$CLICKHOUSE_CLIENT -q "
    SYSTEM START REPLICATION QUEUES r2;
    DROP TABLE r1, r2;
"
