#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `enable_robin_hood_hash_for_joins = 'auto'` the hash tables of a `parallel_hash` join grow as usual until growing would take
# the join over the spill threshold or over `max_memory_usage`. Then the buckets switch to the Robin Hood mode in place, which
# needs no memory, and the join stays in memory where it would spill or fail.
#
# 6 million keys are about 23 thousand keys in each of the 256 buckets, below the 32768 keys at which a bucket switches with
# `enable_robin_hood_hash_for_joins = 1`. A join with `1` is therefore as large as with `0`, and only `'auto'` switches. The limits
# come from the peak memory of the join with `0`, because the sizes of the tables differ between builds.
KEYS=6000000

# The settings that the sizes depend on. The test runner randomizes some of them, and a `SETTINGS` clause overrides the session.
declare -A BASE_SETTINGS=(
    [join_algorithm]="'parallel_hash'"
    [hash_join_layout]="'parallel'"
    [parallel_hash_join_threshold]=0
    [query_plan_join_swap_table]=0
    [enable_join_runtime_filters]=0
    [max_threads]=4
    [max_block_size]=65409
    [max_joined_block_size_rows]=65409
    [join_output_by_rowlist_perkey_rows_threshold]=5
    [max_bytes_before_external_join]=0
    [max_bytes_ratio_before_external_join]=0
    [max_memory_usage]=0
    [collect_hash_table_stats_during_joins]=0
)

QUERY="SELECT count(), sum(r.v) FROM (SELECT intHash64(intHash64(number) % $KEYS) AS k FROM numbers(1000000)) AS l
    INNER JOIN (SELECT intHash64(number) AS k, number AS v FROM numbers($KEYS)) AS r ON l.k = r.k"

# Runs the query under the base settings changed by the arguments `name=value`, and prints its result, or the name of the exception
# for a memory limit. The label names the query in the query log.
run()
{
    local label=$1
    shift
    local -A settings
    local key override clause=""
    for key in "${!BASE_SETTINGS[@]}"; do settings[$key]=${BASE_SETTINGS[$key]}; done
    for override in "$@"; do settings[${override%%=*}]=${override#*=}; done
    for key in "${!settings[@]}"; do clause+="${clause:+, }$key = ${settings[$key]}"; done

    local output
    output=$($CLICKHOUSE_CLIENT --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${label}" --query "$QUERY SETTINGS $clause" 2>&1)
    if [[ "$output" == *MEMORY_LIMIT_EXCEEDED* ]]; then echo "MEMORY_LIMIT_EXCEEDED"; else echo "$output"; fi
}

mode_setting()
{
    if [ "$1" = auto ]; then echo "enable_robin_hood_hash_for_joins='auto'"; else echo "enable_robin_hood_hash_for_joins=$1"; fi
}

# Prints the expression for the query of `run` with that label, from the query log. Call `flush_logs` first.
logged()
{
    $CLICKHOUSE_CLIENT --query "SELECT $2 FROM system.query_log WHERE current_database = currentDatabase()
        AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1' AND type != 'QueryStart' ORDER BY event_time_microseconds DESC LIMIT 1"
}

flush_logs()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
}

# Prints "the same result" for the result of the join with `0`, and the result itself otherwise.
compare()
{
    if [ "$1" = "$EXPECTED" ]; then echo "the same result"; else echo "$1"; fi
}

EXPECTED=$(run peak "$(mode_setting 0)")
echo "result: $EXPECTED"
flush_logs
PEAK=$(logged peak memory_usage)
LIMIT=$((PEAK * 8 / 10))

echo "--- the join spills at 0.8 of the peak memory"
for mode in 0 auto
do
    echo "mode $mode: $(compare "$(run "spill_$mode" "$(mode_setting "$mode")" max_bytes_before_external_join=$((LIMIT * 2)))")"
done
flush_logs
for mode in 0 auto
do
    echo "mode $mode: spilled $(logged "spill_$mode" "ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']"), switched $(logged "spill_$mode" "ProfileEvents['JoinRobinHoodSwitchesOnMemoryPressure'] > 0")"
done

echo "--- max_memory_usage is at 0.8 of the peak memory"
for mode in 0 1 auto
do
    echo "mode $mode: $(compare "$(run "memory_$mode" "$(mode_setting "$mode")" max_memory_usage=$LIMIT)")"
done
flush_logs
echo "mode auto: switched $(logged memory_auto "ProfileEvents['JoinRobinHoodSwitchesOnMemoryPressure'] > 0")"

# With room for the growth the join must not switch. A bucket that already grew is in the memory usage, so the check must not
# count its growth again for the buckets that ask after it.
echo "--- max_memory_usage is at 1.15 of the peak memory"
echo "mode auto: $(compare "$(run headroom_auto "$(mode_setting auto)" max_memory_usage=$((PEAK * 115 / 100)))")"
flush_logs
echo "mode auto: switched $(logged headroom_auto "ProfileEvents['JoinRobinHoodSwitchesOnMemoryPressure'] > 0")"

echo "--- two runs with the hash table statistics on"
# The second run is sized from the statistics of the first when the server still has them: a join stores them when it is
# destroyed, and the cache is shared with every other query. Either way neither run may spill.
SAME_RESULT=1
for attempt in 1 2
do
    [ "$(run "hint_$attempt" "$(mode_setting auto)" max_bytes_before_external_join=$((LIMIT * 2)) collect_hash_table_stats_during_joins=1)" = "$EXPECTED" ] || SAME_RESULT=0
done
flush_logs
echo "the same result in both runs: $SAME_RESULT"
echo "spilled: $(logged hint_1 "ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']") $(logged hint_2 "ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']")"
