#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# By default, a bucket of the hash tables of a hash join switches to the Robin Hood mode in place when growing would take the
# join over the spill threshold or the memory limit, and the join halves its buckets in that mode when it reaches the spill
# threshold later. Both move the cells of the bucket, so every join kind and map type must return the result of the same
# join without that pressure. ASOF is not here: it never spills, and its memory peaks after the build, so neither path
# applies to it.

SETTINGS="join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', parallel_hash_join_threshold = 0,
    query_plan_join_swap_table = 0, enable_join_runtime_filters = 0, enable_hash_join_row_store = 0, max_threads = 1,
    max_block_size = 65409, max_joined_block_size_rows = 65409, collect_hash_table_stats_during_joins = 0,
    max_bytes_ratio_before_external_join = 0, max_memory_usage = 0, max_bytes_in_join = 0, max_rows_in_join = 0,
    grace_hash_join_initial_buckets = 1, enable_parallel_replicas = 0, query_plan_optimize_join_order_randomize = 0,
    join_use_nulls = 0"

LEFT="(SELECT intHash64(number) AS k, number AS k2, toString(number) AS s FROM numbers(75000, 150000))"

# Prints the right side: `rows` rows with `keys` distinct keys, which all come in its first `keys` rows.
right()
{
    echo "(SELECT intHash64(number % $1) AS k, number % $1 AS k2, toString(number % $1) AS s, number AS v FROM numbers($2))"
}

# Prints whether the query with that label spilled, switched in place and halved.
events()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "SELECT 'spilled ' || toString(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'])
        || ', switched ' || toString(ProfileEvents['JoinRobinHoodSwitchesOnMemoryPressure'] > 0)
        || ', halved ' || toString(ProfileEvents['JoinRobinHoodHalvedBuckets'] > 0)
        FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1'
        AND type = 'QueryFinish'"
}

# Runs a query, with the settings `extra`, at a spill threshold of `percent` of twice the bytes that the join ends with when
# nothing switches, and prints whether it returns the result of a join that does not spill.
with_spill_threshold()
{
    local label=$1 percent=$2 query=$3 extra=${4:+, $4}
    # `enable_robin_hood_hash_for_joins = 1` switches only hash tables much larger than these. `SpillingHashJoin` logs the
    # bytes of the join when they fit in memory.
    local expected full
    expected=$($CLICKHOUSE_CLIENT --query "$query SETTINGS $SETTINGS$extra, enable_robin_hood_hash_for_joins = 1,
        max_bytes_before_external_join = 100000000000, send_logs_level = 'debug'" 2>"$CLICKHOUSE_TMP/${label}.log")
    full=$(grep -oE 'All blocks fit in memory \([0-9]+ bytes' "$CLICKHOUSE_TMP/${label}.log" | grep -oE '[0-9]+' | head -n 1)
    local result
    result=$($CLICKHOUSE_CLIENT --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}" --query "$query SETTINGS $SETTINGS$extra,
        enable_robin_hood_hash_for_joins = 0, max_bytes_before_external_join = $((full * 2 * percent / 100))")
    echo "$label: $([ "$result" = "$expected" ] && echo 'the same result' || echo "$result instead of $expected"), $(events "$label")"
}

# The join spills before a block once its bytes reach half of the threshold.
for variant in halved switched
do
    if [ "$variant" = halved ]
    then
        echo "--- the rows after the last growth take the join over the spill threshold"
        # All the keys come in the first quarter of the right side, so the hash tables stop growing there, and the rows
        # after that take the join over the threshold.
        R=$(right 150000 600000)
        percent=93
    else
        echo "--- a growth step would take the join over the spill threshold"
        # Every right row has a new key, so the hash tables grow until about 87% of the right side, and that last growth
        # would take the join past 80% of the bytes it ends with.
        R=$(right 300000 300000)
        percent=80
    fi
    with_spill_threshold "right_$variant" $percent "SELECT count(), sum(cityHash64(l.k, r.v)) FROM $LEFT AS l RIGHT JOIN $R AS r ON l.k = r.k"
    with_spill_threshold "full_$variant" $percent "SELECT count(), sum(cityHash64(l.k, r.v)) FROM $LEFT AS l FULL JOIN $R AS r ON l.k = r.k"
    # No right column is selected, so the hash tables store the keys alone.
    with_spill_threshold "anti_$variant" $percent "SELECT count(), sum(cityHash64(l.k)) FROM $LEFT AS l LEFT ANTI JOIN $R AS r ON l.k = r.k"
    STRING_QUERY="SELECT count(), sum(cityHash64(l.s, r.v)) FROM $LEFT AS l INNER JOIN $R AS r ON l.s = r.s"
    with_spill_threshold "packed_string_$variant" $percent "$STRING_QUERY" "enable_packed_string_keys_in_hash_join = 1"
    # Without packing, the hash tables store the strings with their hashes.
    with_spill_threshold "string_$variant" $percent "$STRING_QUERY" "enable_packed_string_keys_in_hash_join = 0"
done

# Runs a query at a memory limit of `percent` of its peak memory when nothing switches, and prints whether it returns the
# result of the query without a limit. A join with `OR` does not spill.
with_memory_limit()
{
    local label=$1 percent=$2 query=$3
    local settings="$SETTINGS, max_bytes_before_external_join = 0, max_memory_usage_for_user = 0"
    local expected peak result
    expected=$($CLICKHOUSE_CLIENT --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}_peak" --query "$query SETTINGS $settings,
        enable_robin_hood_hash_for_joins = 1")
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    peak=$($CLICKHOUSE_CLIENT --query "SELECT memory_usage FROM system.query_log WHERE current_database = currentDatabase()
        AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}_peak' AND type = 'QueryFinish'")
    result=$($CLICKHOUSE_CLIENT --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}" --query "$query SETTINGS $settings,
        enable_robin_hood_hash_for_joins = 0, max_memory_usage = $((peak * percent / 100))" 2>&1 \
        | grep -oE 'MEMORY_LIMIT_EXCEEDED|^[0-9]+\s+[0-9]+')
    echo "$label: $([ "$result" = "$expected" ] && echo 'the same result' || echo "$result"), $(events "$label")"
}

echo "--- two disjuncts, so two hash tables that grow together, at a memory limit of 0.75 of the peak memory"
with_memory_limit or 75 "SELECT count(), sum(cityHash64(l.k, r.v))
    FROM (SELECT intHash64(number) AS k, number AS k2 FROM numbers(75000, 2000)) AS l
    INNER JOIN $(right 300000 300000) AS r ON l.k = r.k OR l.k2 = r.k2 + 7"

echo "--- two disjuncts, one with 10 keys, at a memory limit of 1.25 of the peak memory"
# Only the hash table of `k` grows, and it fits: the hash table of `k2` must not be counted as growing with it.
with_memory_limit or_few_keys 125 "SELECT count(), sum(cityHash64(l.k, r.v))
    FROM (SELECT intHash64(number) AS k, number % 10 + 100 AS k2 FROM numbers(75000, 2000)) AS l
    INNER JOIN (SELECT intHash64(number) AS k, number % 10 AS k2, number AS v FROM numbers(300000)) AS r
    ON l.k = r.k OR l.k2 = r.k2"
