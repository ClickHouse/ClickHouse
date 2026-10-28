#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide.
# no-fasttest: a test that arms a fail point runs alone, and such tests are kept out of the fast test.
# no-random-settings: read limits and overflow modes disable the optimization.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The passes of `query_plan_two_pass_count_top_k` run in the pipeline of the query: KILL QUERY and
# max_execution_time stop the query between the passes. The fail point pauses it after the pilot.

FP=two_pass_count_top_k_pause_between_passes

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_two_pass_cancel;
    CREATE TABLE t_two_pass_cancel (k UInt64, s String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, index_granularity_bytes = 0;
    INSERT INTO t_two_pass_cancel SELECT
        multiIf(number % 10 = 0, intHash64(number) % 20, number % 10 < 4, 1000 + intHash64(number) % 3000, 100000 + intHash64(number) % 300000),
        multiIf(number % 10 = 0, toString(intHash32(number) % 5), toString(intHash32(number) % 50))
    FROM numbers(1000000);
"

SETTINGS="query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0, enable_parallel_replicas = 0"
QUERY="SELECT k, s, count() FROM t_two_pass_cancel GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10"
OUT="${CLICKHOUSE_TMP}/two_pass_cancel_${CLICKHOUSE_DATABASE}.out"

echo "KILL QUERY"
query_id="two_pass_kill_${CLICKHOUSE_DATABASE}_${RANDOM}"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "$QUERY SETTINGS $SETTINGS" > "$OUT" 2>&1 &
query_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $query_pid
grep -q "QUERY_WAS_CANCELLED" "$OUT" && echo "cancelled"

echo "max_execution_time"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $SETTINGS, max_execution_time = 1" > "$OUT" 2>&1 &
query_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
sleep 2
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $query_pid
grep -q "TIMEOUT_EXCEEDED" "$OUT" && echo "timeout"

echo "result"
diff <($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $SETTINGS") <($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS query_plan_two_pass_count_top_k = 0") && echo "same"

rm -f "$OUT"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_two_pass_cancel"
