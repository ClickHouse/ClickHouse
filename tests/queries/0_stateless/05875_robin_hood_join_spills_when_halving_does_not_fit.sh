#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs a build with libfiu to enable the failpoint.
# no-parallel: the failpoint is server-wide and fires once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Halving allocates the new bucket buffers before it frees the old ones. When they do not fit in the memory the query can
# still allocate, the join spills, as it does without halving, and returns the same result. The failpoint makes them not fit.

# The right table has two blocks: 1.5 million keys, then the same keys with long strings. The hash tables stop growing in the
# first block, and the join stays below half of the threshold until the second, so only the check after the last block, at
# the full threshold, sees the bytes of the join. There, halving would keep it in memory.
$CLICKHOUSE_CLIENT --query "CREATE TABLE right_blocks (k UInt64, v UInt64, s String) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO right_blocks SELECT intHash64(number), number, '' FROM numbers(1500000)
    SETTINGS max_block_size = 2000000, max_insert_block_size = 2000000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0"
$CLICKHOUSE_CLIENT --query "INSERT INTO right_blocks SELECT intHash64(number), number, repeat('x', 130) FROM numbers(1500000)
    SETTINGS max_block_size = 2000000, max_insert_block_size = 2000000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0"

QUERY="SELECT count(), sum(r.v), sum(length(r.s)) FROM (SELECT intHash64(number) AS k FROM numbers(100000)) AS l
    INNER JOIN right_blocks AS r ON l.k = r.k"
SETTINGS="join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', parallel_hash_join_threshold = 0,
    query_plan_join_swap_table = 0, enable_join_runtime_filters = 0, enable_hash_join_row_store = 0, max_threads = 1,
    max_block_size = 65409, max_joined_block_size_rows = 65409, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    collect_hash_table_stats_during_joins = 0, max_bytes_ratio_before_external_join = 0, max_memory_usage = 0,
    max_bytes_in_join = 0, max_rows_in_join = 0, grace_hash_join_initial_buckets = 1, enable_parallel_replicas = 0,
    query_plan_optimize_join_order_randomize = 0, enable_robin_hood_hash_for_joins = 0"

EXPECTED=$($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $SETTINGS")
# `SpillingHashJoin` logs the bytes of the join when they fit in memory. Halving frees about 11% of them.
FULL=$($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS $SETTINGS, max_bytes_before_external_join = 100000000000, send_logs_level = 'debug'" 2>&1 \
    | grep -oE 'All blocks fit in memory \([0-9]+ bytes' | grep -oE '[0-9]+' | head -n 1)

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT spilling_hash_join_halving_does_not_fit"
result=$($CLICKHOUSE_CLIENT --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_join" --query "$QUERY SETTINGS $SETTINGS,
    max_bytes_before_external_join = $((FULL * 95 / 100))")
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT spilling_hash_join_halving_does_not_fit"
echo "$([ "$result" = "$EXPECTED" ] && echo 'the same result' || echo "$result")"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT 'spilled ' || toString(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'])
    || ', halved ' || toString(ProfileEvents['JoinRobinHoodHalvedBuckets'] > 0)
    FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_join'
    AND type = 'QueryFinish'"
