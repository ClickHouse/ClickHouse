#!/usr/bin/env bash
# Tags: long

# Automatic parallel replicas give the replicas plan the index analysis of the single-node plan. Without a shared
# snapshot the two plans take one each, and a mutation submitted between them is applied on the fly by one plan only.
# The analysis then selects the ranges of one state of the table for a read of another, so the optimization declines.
# Either state holds the same answer, so the query has to return it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS=(
    --enable_analyzer=1 --enable_parallel_replicas=1 --max_parallel_replicas=3 --automatic_parallel_replicas_mode=1
    --cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost
    --parallel_replicas_for_non_replicated_merge_tree=1 --parallel_replicas_local_plan=1
    --automatic_parallel_replicas_min_bytes_per_replica=1 --merge_tree_min_bytes_per_task_for_remote_reading=1
    --max_threads=16 --max_threads_min_free_memory_per_thread=0 --use_query_condition_cache=0 --serialize_query_plan=0
    --enable_shared_storage_snapshot_in_query=0 --apply_mutations_on_fly=1
)
QUERY="SELECT count() FROM t_autopr_race WHERE val < 100 AND pad >= 0"

function run_with_mutation_between_snapshots()
{
    $CLICKHOUSE_CLIENT -m --query "
        DROP TABLE IF EXISTS t_autopr_race;
        CREATE TABLE t_autopr_race (id UInt64, val UInt64, pad UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
        SYSTEM STOP MERGES t_autopr_race;
        INSERT INTO t_autopr_race SELECT number, intDiv(number, 1000), number FROM numbers(500000);
    "
    # Collect the statistics, so that the next run decides on them.
    $CLICKHOUSE_CLIENT "${SETTINGS[@]}" --query "$QUERY" > /dev/null

    local query_id="${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}_race"
    $CLICKHOUSE_CLIENT "${SETTINGS[@]}" --merge_tree_storage_snapshot_sleep_ms=3000 --query_id="$query_id" --query "$QUERY" &
    local pid=$!

    # The single-node plan has taken its snapshot once the plan with parallel replicas sleeps before taking another.
    local found=0
    for _ in $(seq 1 100); do
        if [[ $($CLICKHOUSE_CLIENT --query "
            SYSTEM FLUSH LOGS text_log;
            SELECT count() FROM system.text_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$query_id'
              AND message_format_string = 'Injecting {}ms artificial delay before taking storage snapshot'") -ge 2 ]]; then
            found=1
            break
        fi
        sleep 0.1
    done
    if [[ $found -eq 1 ]]; then
        $CLICKHOUSE_CLIENT --mutations_sync=0 --query "ALTER TABLE t_autopr_race UPDATE pad = pad + 1 WHERE 1"
    fi
    wait $pid
    if [[ $found -eq 1 ]]; then
        $CLICKHOUSE_CLIENT --query "
            SYSTEM FLUSH LOGS query_log;
            SELECT 'applied', ProfileEvents['AutoParallelReplicasApplied'] FROM system.query_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$query_id' AND type = 'QueryFinish'"
    fi
    echo "found=$found"
}

for _ in {1..5}; do
    result=$(run_with_mutation_between_snapshots)
    if [[ "$result" == *"found=1" ]]; then
        break
    fi
done
# A run that could not place the mutation between the snapshots says nothing about the decision, so fail rather than
# pass without having exercised it.
if [[ "$result" != *"found=1" ]]; then
    echo "The mutation could not be placed between the snapshots of the two plans in any attempt"
    $CLICKHOUSE_CLIENT --query "DROP TABLE t_autopr_race"
    exit 1
fi
echo "$result" | grep -v '^found='

$CLICKHOUSE_CLIENT --query "DROP TABLE t_autopr_race"
