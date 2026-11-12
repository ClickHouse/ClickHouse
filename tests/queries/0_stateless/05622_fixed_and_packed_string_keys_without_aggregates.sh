#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `GROUP BY` of fixed-width keys and one `String` key without aggregate functions takes the key-only forms
# of the methods of such keys (`keys32_packed_string_void`, `keys64_packed_string_void`,
# `keys128_packed_string_void`), whose cells hold the keys alone. The keys are packed into 1, 5 and 9 bytes,
# and a `LowCardinality(String)` and a `LowCardinality(UInt32)` key take the same methods. The strings are
# empty, short or longer than the inline limit. Each check compares the result with the one of the serialized
# method of the ordinary aggregator. The profiles merge the single-level tables of the threads in hash
# partitions or serially, convert the tables to two levels, write them to disk and merge the blocks back with
# the serialized method, aggregate in order of the sorting key, and stop adding keys under a group limit. The
# adaptive aggregator freezes its tables after 16 keys and stages the other keys as key-only records, which it
# writes to disk; it does not take `LowCardinality` keys. That check runs in a clickhouse-local process of its
# own, so the counters in `system.events` belong to it alone.

data="$CLICKHOUSE_TMP/data_05622"
rm -rf "$data"
$CLICKHOUSE_LOCAL --path "$data" --query "
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE TABLE t (u8 UInt8, u32 UInt32, u64 UInt64, s String, ls LowCardinality(String), lu32 LowCardinality(UInt32))
    ENGINE = MergeTree ORDER BY (u64, u8, s) SETTINGS index_granularity = 8192;
    INSERT INTO t SELECT number % 7, intDiv(number, 3) % 20000, intDiv(number, 3) % 50000,
        multiIf(number % 11 = 0, '', number % 11 = 1, repeat('x', 16 + number % 20), toString(intDiv(number, 7) % 3000)) AS str,
        str, number % 300
    FROM numbers(100000);"

keys=("u8, s" "u32, u8, s" "u64, u8, s" "u32, ls" "lu32, s")

# The table is read in 4 streams, each into a table of its own, despite its size.
read_in_streams="SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192, optimize_aggregation_in_order = 0,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;"

# Prints the label and whether the result of `GROUP BY $2` equals the one of the serialized method of the ordinary
# aggregator, under the settings of the session and the extra settings `$3` of both queries.
function compare()
{
    local label=$1 keys=$2 settings=${3:+, $3}
    echo "SELECT '$label', '$keys',
        (SELECT count(), sum(cityHash64(*)) FROM (SELECT $keys FROM t GROUP BY $keys)
            SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1$settings)
        = (SELECT count(), sum(cityHash64(*)) FROM (SELECT $keys FROM t GROUP BY $keys)
            SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0$settings);"
}

echo "methods"
methods=""
for k in "${keys[@]}"; do
    methods+="SELECT $k FROM t GROUP BY $k SETTINGS log_comment = '$k' FORMAT Null;"
done
methods+="SELECT u32, u8, s FROM t GROUP BY u32, u8, s SETTINGS log_comment = 'setting off', enable_fixed_and_packed_string_keys_in_aggregation = 0 FORMAT Null;"
methods+="SELECT u32, u8, s, count() FROM t GROUP BY u32, u8, s SETTINGS log_comment = 'with count' FORMAT Null;"
$CLICKHOUSE_LOCAL --path "$data" --send_logs_level=trace --query "$methods" 2>&1 \
    | grep -oE "comment: [a-z0-9_ ,]+|Aggregation method: [a-z0-9_]+" | uniq

echo "profiles"
# The settings of each profile go to its queries, so the profiles do not add up. Each profile checks one key set, and
# together they cover every width and both `LowCardinality` keys; `u64, u8, s` is a prefix of the sorting key, which
# aggregation in order needs.
declare -A profile_keys=(
    ["partitioned merge"]="u64, u8, s"
    ["serial merge"]="u8, s"
    ["two levels"]="u32, u8, s"
    ["external aggregation"]="u32, ls"
    ["in order"]="u64, u8, s"
    ["group limit"]="lu32, s"
)
declare -A profiles=(
    ["partitioned merge"]="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 1"
    ["serial merge"]="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, enable_parallel_single_level_merge = 0"
    ["two levels"]="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1"
    ["external aggregation"]="enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0"
    ["in order"]="optimize_aggregation_in_order = 1"
    ["group limit"]="max_threads = 1, max_rows_to_group_by = 1000, group_by_overflow_mode = 'any'"
)
queries="$read_in_streams"
for profile in "partitioned merge" "serial merge" "two levels" "external aggregation" "in order" "group limit"; do
    queries+=$(compare "$profile" "${profile_keys[$profile]}" "${profiles[$profile]}")
done
$CLICKHOUSE_LOCAL --path "$data" --query "$queries"

echo "adaptive"
queries="$read_in_streams
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 16, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;"
for k in "u32, u8, s" "u64, u8, s"; do
    queries+=$(compare "adaptive" "$k")
done
queries+="SELECT 'adaptive, staged and written to disk', sumIf(value, event = 'AdaptiveAggregationStagedRecords') > 0,
    sumIf(value, event = 'AdaptiveAggregationSpills') > 0 FROM system.events;"
$CLICKHOUSE_LOCAL --path "$data" --query "$queries"

rm -rf "$data"
