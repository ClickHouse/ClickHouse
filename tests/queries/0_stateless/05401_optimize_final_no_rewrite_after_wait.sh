#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`,
#   which would pause projection merges of other tests running at the same time.
# - no-fasttest: relies on failpoints, which are not available in the fast test build.

# OPTIMIZE ... FINAL that finds the parts of a partition being merged waits for that merge and retries.
# If the awaited merge has merged the whole partition into a single part, the retry must not merge that
# part with itself once more. OPTIMIZE ... FINAL DEDUPLICATE still does, because the awaited merge did
# not deduplicate. A merge is held in flight with a failpoint, so the wait happens deterministically.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table=t_optimize_final_after_wait

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table SYNC" 2>/dev/null
}
trap cleanup EXIT

function run_case()
{
    local second_optimize=$1
    local second_query_id=$2
    echo "$second_optimize"

    # A projection makes every merge go through the projection-merge stage, where the failpoint pauses it.
    # `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from these parts.
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table SYNC"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table (k UInt64, v UInt64, PROJECTION by_v (SELECT k, v ORDER BY v))
        ENGINE = MergeTree ORDER BY k
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, deduplicate_merge_projection_mode = 'rebuild'"

    for i in 1 2 3; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO $table SELECT number, $i FROM numbers(1000)"
    done

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $table FINAL" &
    local first_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"

    # The second OPTIMIZE cannot take the parts of the paused merge, so it waits for that merge. The client
    # receives the server logs of the query only after it finishes, so look for the wait in `system.text_log`.
    $CLICKHOUSE_CLIENT --query_id "$second_query_id" --query "$second_optimize" &
    local second_pid=$!

    local waiting=no
    for _ in {1..120}; do
        if [[ $($CLICKHOUSE_CLIENT --query "
            SYSTEM FLUSH LOGS text_log;
            SELECT count() FROM system.text_log
            WHERE event_date >= yesterday() AND query_id = '$second_query_id'
                AND message LIKE 'Waiting for currently running merges%'") -ge 1 ]]; then
            waiting=yes
            break
        fi
        sleep 0.5
    done
    echo "second OPTIMIZE waits for the running merge: $waiting"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
    wait "$first_pid"
    wait "$second_pid"

    # Level 1: the part made by the first OPTIMIZE is kept. Level 2: it was merged with itself once more.
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), max(level)
        FROM system.parts
        WHERE database = currentDatabase() AND table = '$table' AND active"
}

run_case "OPTIMIZE TABLE $table FINAL" "${CLICKHOUSE_DATABASE}_final"
run_case "OPTIMIZE TABLE $table FINAL DEDUPLICATE" "${CLICKHOUSE_DATABASE}_final_deduplicate"
