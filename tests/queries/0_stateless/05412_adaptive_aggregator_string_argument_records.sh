#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# A frozen table of the adaptive aggregator stages the arguments of its missed rows in records, and the merge rebuilds
# them for the aggregate functions. A `String` argument is staged as a 4-byte length and its bytes. The records of a
# fixed-width key whose variable-size arguments are all `String`s have no header, so their size follows from the
# lengths. Other argument sets take the general records, where the `String`s sit beside values in their columns'
# serialization. The first cell stages several `String` arguments in one header-less record, with empty and long values
# and `LowCardinality` and constant sources. The second puts serialized values before and after `String`s in general
# records, so the drain resumes the `String`s where a serialized value ends. A threshold of 32 keys freezes the tables
# almost at once, so nearly every row is staged. Both runs of a cell share a clickhouse-local process, the adaptive one
# first, so the counters in `system.events` belong to it alone: the run with the feature off adds none. Every cell
# compares the results of all groups with the feature off and reports whether the adaptive run staged records. The
# settings that the staging depends on are pinned.
s1="repeat('a', number % 7)"
s2="concat(toString(number), repeat('b', number % 300))"
strings="argMax(${s1}, number), min(${s2}), uniqExact(${s1}, ${s2}),
    max(toLowCardinality(toString(number % 100))), max('constant')"
mixed="max([number]), max(${s1}), max(if(number % 3 = 0, NULL, ${s2})), min(${s2})"

check()
{
    local label=$1 key=$2 aggregates=$3
    local query="SELECT sum(cityHash64(*)) FROM (SELECT ${key} AS k, ${aggregates} FROM numbers_mt(40123) GROUP BY k)"
    $CLICKHOUSE_LOCAL --query "
        SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8191;
        SET optimize_injective_functions_in_group_by = 0;
        SET adaptive_aggregator_freeze_threshold = 32, adaptive_aggregator_freeze_threshold_bytes = 0;
        SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
        SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
        SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
        ${query} SETTINGS enable_adaptive_aggregator = 1 INTO OUTFILE '${CLICKHOUSE_TMP}/string_argument_records_1.out' TRUNCATE;
        ${query} SETTINGS enable_adaptive_aggregator = 0 INTO OUTFILE '${CLICKHOUSE_TMP}/string_argument_records_0.out' TRUNCATE;
        SELECT '${label}', sumIf(value, event = 'AdaptiveAggregationStagedRecords') > 0 FROM system.events;
    "
    diff -u "${CLICKHOUSE_TMP}/string_argument_records_0.out" "${CLICKHOUSE_TMP}/string_argument_records_1.out"
}

check 'Strings without a header' 'toUInt64(number % 5000)' "${strings}"
check 'Strings among serialized values' 'toUInt64(number % 5000)' "${mixed}"
