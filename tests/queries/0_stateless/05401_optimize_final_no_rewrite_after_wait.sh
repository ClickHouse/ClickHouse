#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the test toggles the server-global failpoints `merge_task_projection_stage_pause` and
#   `mt_optimize_final_pause_after_wait`, which would pause merges and OPTIMIZE of other tests running at the same time.
# - no-fasttest: relies on failpoints, which are not available in the fast test build.

# OPTIMIZE ... FINAL that finds the parts of a partition being merged waits for that merge and retries.
# If the awaited merge has merged the whole partition into a single part, the retry must not merge that
# part with itself once more, unless the merge again would do more than the awaited merge did.
# A merge is held in flight with a failpoint, so the wait happens deterministically.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

table=t_optimize_final_after_wait

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_optimize_final_pause_after_wait" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table SYNC" 2>/dev/null
}
trap cleanup EXIT

case_number=0

# Optional variables: `ttl` (table TTL clause), `alter` (run before the second OPTIMIZE starts),
# `mutation` (run while the second OPTIMIZE waits; the retry happens only after the mutation is done).
function run_case()
{
    local label=$1
    local second_optimize=$2
    local second_query_id="${CLICKHOUSE_DATABASE}_$((++case_number))"
    echo "$label"

    # A projection makes every merge go through the projection-merge stage, where the failpoint pauses it.
    # `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from these parts.
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table SYNC"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table (k UInt64, v UInt64, d DateTime DEFAULT now(), PROJECTION by_v (SELECT k, v ORDER BY v))
        ENGINE = MergeTree ORDER BY k ${ttl:-}
        SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, deduplicate_merge_projection_mode = 'rebuild'"

    for i in 1 2 3; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO $table (k, v) SELECT number, $i FROM numbers(1000)"
    done

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $table FINAL" &
    local first_pid=$!
    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"

    if [[ -n "${alter:-}" ]]; then
        $CLICKHOUSE_CLIENT --query "$alter"
    fi

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

    local mutation_pid=
    if [[ -n "${mutation:-}" ]]; then
        $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_optimize_final_pause_after_wait"
        $CLICKHOUSE_CLIENT --query "$mutation" --mutations_sync 1 &
        mutation_pid=$!
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"

    if [[ -n "$mutation_pid" ]]; then
        # The mutation runs on the part made by the awaited merge while the retry of the second OPTIMIZE is held.
        wait "$mutation_pid"
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_optimize_final_pause_after_wait"
    fi

    wait "$first_pid"
    wait "$second_pid"

    # Level 1: the part made by the first OPTIMIZE is kept. Level 2: it was merged with itself once more.
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), max(level)
        FROM system.parts
        WHERE database = currentDatabase() AND table = '$table' AND active"
}

run_case "FINAL" "OPTIMIZE TABLE $table FINAL"
# The awaited merge did not deduplicate.
run_case "FINAL DEDUPLICATE" "OPTIMIZE TABLE $table FINAL DEDUPLICATE"
# Rows could expire after the awaited merge started.
ttl="TTL d + INTERVAL 1 YEAR" run_case "FINAL with TTL" "OPTIMIZE TABLE $table FINAL"
# The awaited merge used the old metadata.
alter="ALTER TABLE $table ADD COLUMN x UInt64 DEFAULT 7" run_case "FINAL after ALTER" "OPTIMIZE TABLE $table FINAL"
# The mutation may leave work for a merge.
mutation="ALTER TABLE $table UPDATE d = d + 1 WHERE 1" run_case "FINAL after mutation of the merged part" "OPTIMIZE TABLE $table FINAL"
