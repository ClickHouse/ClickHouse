#!/usr/bin/env bash

# A hash join with the parallel layout keeps a record of every waiting block until its build. For a condition in `ON`
# on the right side, the record keeps the mask of the condition, which `getColumnAsMask` allocates for a `Nullable`
# column. The bytes that the join reports must count that mask, because `max_bytes_before_external_join` is compared
# with them. Both joins below store the right columns `k` and `flag`, and only the second has the condition `r.flag`.
# Every 100th right key is NULL, so that both records keep a skip byte per row. At build finish the second join then
# holds at least one byte more for each of the 200000 right rows: its mask.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE VIEW right_rows AS
    SELECT if(number % 100 = 0, NULL, toUInt32(number)) AS k, toNullable(toUInt8(number < 1000000)) AS flag
    FROM numbers(200000)"
$CLICKHOUSE_CLIENT -q "CREATE VIEW left_keys AS SELECT toUInt32(number * 2) AS k FROM numbers(100000)"

settings="join_algorithm = 'hash', hash_join_layout = 'parallel', max_bytes_before_external_join = 1000000000,
    max_bytes_ratio_before_external_join = 0, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 8000, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    parallel_hash_join_threshold = 0, max_rows_in_join = 0, max_bytes_in_join = 0, enable_robin_hood_hash_for_joins = 0,
    join_use_nulls = 0, collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0"

# The bytes that the join holds at build finish, as `HashJoin::evaluateBuildFit` logs them.
held_bytes()
{
    $CLICKHOUSE_CLIENT --send_logs_level=trace -q "
        SELECT count(), sum(r.k), sum(r.flag) FROM left_keys AS l FULL ALL JOIN right_rows AS r ON $1
        SETTINGS $settings FORMAT Null" 2>&1 | grep -oE 'Build fit: held = [0-9]+' | grep -oE '[0-9]+$'
}

without_condition=$(held_bytes "l.k = r.k")
with_condition=$(held_bytes "l.k = r.k AND r.flag")
echo $(( with_condition - without_condition >= 200000 ))

$CLICKHOUSE_CLIENT -q "DROP VIEW left_keys"
$CLICKHOUSE_CLIENT -q "DROP VIEW right_rows"
