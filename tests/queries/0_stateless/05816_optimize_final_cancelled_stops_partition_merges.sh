#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree
# - no-parallel: the server-global failpoint `merge_task_projection_stage_pause` pauses the projection merges of every table.
# - no-fasttest: failpoints are not available in the fast test build.
# - no-shared-merge-tree: checks the per-partition loop of `OPTIMIZE ... FINAL` of `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# OPTIMIZE FINAL merges at most `background_pool_size` partitions at once, so with two more partitions
# some can only start after another partition's merge finished.
pool_size=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")
num_partitions=$((pool_size + 2))

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
    rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_*.out
}
trap cleanup EXIT

# Runs OPTIMIZE FINAL of a new table $1 with query id $2 and extra settings $3, cancels it while the started
# merges are paused (with KILL QUERY if $4 = kill, else by its max_execution_time), then lets them finish.
function run_case()
{
    local table="$1" query_id="$2" settings="$3" kill="$4"
    local out="${CLICKHOUSE_TMP}/${query_id}.out"

    # The projection makes every merge stop at the failpoint. `max_bytes_to_merge_at_max_space_in_pool = 1`
    # keeps background merges away from these parts; OPTIMIZE FINAL merges them regardless of size.
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE $table (p UInt16, k UInt64, PROJECTION agg (SELECT p, count() GROUP BY p))
        ENGINE = MergeTree PARTITION BY p ORDER BY k
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
        INSERT INTO $table SELECT number % $num_partitions, number FROM numbers($num_partitions) SETTINGS max_partitions_per_insert_block = 0;
        INSERT INTO $table SELECT number % $num_partitions, number FROM numbers($num_partitions) SETTINGS max_partitions_per_insert_block = 0;
        SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause;
    "

    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "OPTIMIZE TABLE $table FINAL $settings" > "$out" 2>&1 &
    local client_pid=$!

    # No partition finishes while the failpoint is enabled.
    while clickhouse_test_time_left && kill -0 "$client_pid" 2>/dev/null; do
        [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$table'") -gt 0 ]] && break
        sleep 0.1
    done
    [[ "$kill" == kill ]] && $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' ASYNC FORMAT Null"
    while clickhouse_test_time_left && kill -0 "$client_pid" 2>/dev/null; do
        [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND is_cancelled") -gt 0 ]] && break
        sleep 0.1
    done
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
    wait "$client_pid"

    grep -o -m1 'Code: [0-9]*' "$out"
    # The partitions that had not started when the query was cancelled are not merged.
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(c > 1) > 0 FROM (
            SELECT count() AS c FROM system.parts
            WHERE database = currentDatabase() AND table = '$table' AND active GROUP BY partition)
    "
}

run_case t_kill "${CLICKHOUSE_TEST_UNIQUE_NAME}_kill" "" kill
run_case t_timeout "${CLICKHOUSE_TEST_UNIQUE_NAME}_timeout" "SETTINGS max_execution_time = 3" ""
