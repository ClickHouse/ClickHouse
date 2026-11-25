#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By default a hash join that reaches the spill threshold, or that the memory spill scheduler asks to free memory, rebuilds
# its hash tables in the Robin Hood mode in half their cells when that is enough to stay in memory.
# `enable_robin_hood_hash_for_joins = 1` does not halve, and switches only hash tables much larger than these, so a join
# with `1` spills as a join that never switches does.

# Prints whether the query with that label switched to `GraceHashJoin` and whether it halved hash tables.
events()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "SELECT 'spilled ' || toString(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'])
        || ', halved ' || toString(ProfileEvents['JoinRobinHoodHalvedBuckets'] > 0)
        FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1'
        AND type = 'QueryFinish'"
}

echo "--- the rows after the last growth take the join over the spill threshold"
# All the 1.5 million keys of the right side come in its first quarter, so the hash tables stop growing there, and the rows
# after that take the join over the threshold with no growth step to switch at. One build thread keeps the bytes of the join
# the same in every run.
QUERY="SELECT count(), sum(r.v) FROM (SELECT intHash64(number) AS k FROM numbers(100000)) AS l
    INNER JOIN (SELECT intHash64(number % 1500000) AS k, number AS v FROM numbers(6000000)) AS r ON l.k = r.k"
SETTINGS="join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', parallel_hash_join_threshold = 0,
    query_plan_join_swap_table = 0, enable_join_runtime_filters = 0, enable_hash_join_row_store = 0, max_threads = 1,
    max_block_size = 65409, max_joined_block_size_rows = 65409, collect_hash_table_stats_during_joins = 0,
    max_bytes_ratio_before_external_join = 0, max_memory_usage = 0, max_bytes_in_join = 0, max_rows_in_join = 0,
    grace_hash_join_initial_buckets = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_randomize = 0"

EXPECTED=$($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $SETTINGS, enable_robin_hood_hash_for_joins = 1")
echo "result: $EXPECTED"
# `SpillingHashJoin` logs the bytes of the join when they fit in memory.
FULL=$($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $SETTINGS, enable_robin_hood_hash_for_joins = 1,
    max_bytes_before_external_join = 100000000000, send_logs_level = 'debug'" 2>&1 \
    | grep -oE 'All blocks fit in memory \([0-9]+ bytes' | grep -oE '[0-9]+' | head -n 1)
# Before a block, the join spills once its bytes reach half of the threshold, here 0.93 of the bytes it ends with.
# Halving frees more than the remaining 7%.
THRESHOLD=$((FULL * 2 * 93 / 100))
for mode in 1 0
do
    result=$($CLICKHOUSE_CLIENT --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_threshold_$mode" \
        --query "$QUERY SETTINGS $SETTINGS, enable_robin_hood_hash_for_joins = $mode, max_bytes_before_external_join = $THRESHOLD")
    echo "mode $mode: $([ "$result" = "$EXPECTED" ] && echo 'the same result' || echo "$result")"
    echo "mode $mode: $(events "threshold_$mode")"
done

echo "--- the memory spill scheduler asks the join to free memory"
# The spill threshold is out of reach, so a spill or a halving comes from the scheduler. One build stream reads the right side
# as one block, so the scheduler first sees the join after the whole right side is in.
scheduler_settings=(
    --join_algorithm=parallel_hash --max_threads=1 --max_block_size=1000000
    --max_bytes_before_external_join=100Gi --max_bytes_ratio_before_external_join=0
    --grace_hash_join_initial_buckets=1 --collect_hash_table_stats_during_joins=0
    --max_rows_in_join=0 --max_bytes_in_join=0 --enable_parallel_replicas=0
    --query_plan_join_swap_table=false --query_plan_optimize_join_order_randomize=0
    --max_memory_usage=105Mi --enable_adaptive_memory_spill_scheduler=1
)
scheduler_query="SELECT count(), sum(t2.number) FROM numbers(100) AS t1
    INNER JOIN (SELECT number FROM numbers(600000)) AS t2 ON t1.number = t2.number"
for mode in 1 0
do
    $CLICKHOUSE_CLIENT "${scheduler_settings[@]}" --enable_robin_hood_hash_for_joins="$mode" \
        --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_scheduler_$mode" --query "$scheduler_query"
    echo "mode $mode: $(events "scheduler_$mode")"
done

echo "--- the same with four build streams, which halve the buckets in parallel"
# The memory goes on growing after the halving, so the scheduler may still switch the join to `GraceHashJoin`. The switch frees
# the halved hash tables and replays the stored right blocks. Every right row has a match, so a lost row changes the result.
parallel_settings=(
    --join_algorithm=parallel_hash --max_threads=4 --max_block_size=65536
    --max_bytes_before_external_join=100Gi --max_bytes_ratio_before_external_join=0
    --grace_hash_join_initial_buckets=1 --collect_hash_table_stats_during_joins=0
    --max_rows_in_join=0 --max_bytes_in_join=0 --enable_parallel_replicas=0
    --query_plan_join_swap_table=false --query_plan_optimize_join_order_randomize=0
    --max_memory_usage=120Mi --enable_adaptive_memory_spill_scheduler=1 --enable_robin_hood_hash_for_joins=0
)
$CLICKHOUSE_CLIENT "${parallel_settings[@]}" --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel" --query "SELECT count(), sum(t2.number)
    FROM numbers(1000000) AS t1 INNER JOIN (SELECT number FROM numbers(1000000)) AS t2 ON t1.number = t2.number"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT 'halved ' || toString(ProfileEvents['JoinRobinHoodHalvedBuckets'] > 0) FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel' AND type = 'QueryFinish'"
