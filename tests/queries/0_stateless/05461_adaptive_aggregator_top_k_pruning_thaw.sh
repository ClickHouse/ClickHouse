#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator thaws under the top-K pruning of `ORDER BY count() DESC LIMIT n`, and the
# pruning still holds: a producer that froze counts the groups of its own table into its bins at its finish. In each
# lane the first block holds 4096 distinct keys, which reach the freeze threshold; the next 150000 rows carry 100 keys,
# whose counts grow with the key, all missing the frozen table, so it thaws; the last 150000 rows carry the 10 keys of
# the top, which only the thawed table holds. Each lane is a single stream, so it feeds a producer of its own. The last
# cell is a lone `count()`, whose tables stay frozen under the pruning, on a lane whose middle rows spread 4000 keys over
# every block, so that a thaw would be due if it were allowed.
#
# Every cell compares the rows of the top with those of the same query with the feature off, as a multiset: the top
# has no tie at its boundary. Reading every column keeps the other aggregate, whose records do not combine as those of
# a lone `count()` do. The cell also compares every returned row with the row of its key in the full aggregation. The
# last three columns say whether a table thawed and whether the merge skipped units and staged records. Each cell runs
# in its own clickhouse-local process, so the counters in `system.events` belong to it alone.
#
# The last two blocks keep the hash-table statistics on: a top-K run neither reads the thaw verdict that a run of the
# same aggregation without the `LIMIT` remembers, nor records one.

SETTINGS="SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 4096;
    SET adaptive_aggregator_freeze_threshold = 4096, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;"

function check()
{
    local label=$1 disable_thaw=$2 keys=$3 aggregates=$4
    local middle=${5:-'1000000 + toUInt64(sqrt(number % 10000))'}
    local lane="SELECT multiIf(number < 4096, number, number < 154096, $middle,
        2000000 + number % 10) AS k, number FROM numbers(304096)"
    local source="$lane UNION ALL $lane UNION ALL $lane UNION ALL $lane"
    local query="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys ORDER BY c DESC LIMIT 10"
    local full="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys SETTINGS enable_adaptive_aggregator = 0"
    local columns
    columns=$(echo "$keys, $aggregates" | sed -E 's/[^,]* AS ([a-z]+)/\1/g')
    $CLICKHOUSE_LOCAL --query "
    $SETTINGS
    SET adaptive_aggregator_disable_thaw = $disable_thaw;

    SELECT '$label', $disable_thaw,
        (SELECT arraySort(groupArray(($columns))) FROM ($query))
            = (SELECT arraySort(groupArray(($columns))) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN ($full)) = 0;
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedRecords') > 0;
    " | paste -sd '\t'
}

check 'String key, count and min' 0 'toString(k) AS sk' 'count() AS c, min(number) AS m'
check 'UInt64 key, count and sum' 0 'k' 'count() AS c, sum(number) AS s'
check 'String key, count and min' 1 'toString(k) AS sk' 'count() AS c, min(number) AS m'
check 'String key, lone count' 0 'toString(k) AS sk' 'count() AS c' '1000000 + number % 4000'

LANE="SELECT multiIf(number < 4096, number, number < 154096, 1000000 + toUInt64(sqrt(number % 10000)),
    2000000 + number % 10) AS k, number FROM numbers(304096)"
FULL="SELECT toString(k) AS sk, count() AS c, min(number) AS m
    FROM ($LANE UNION ALL $LANE UNION ALL $LANE UNION ALL $LANE) GROUP BY sk"
TOPK="$FULL ORDER BY c DESC LIMIT 10"
# Each array is [local freezes, thaws].
COUNTERS="SELECT groupArray(value) FROM (SELECT event, coalesce(sum(value), 0) AS value FROM system.events
    WHERE event IN ('AdaptiveAggregationLocalFreezes', 'AdaptiveAggregationThaws') GROUP BY event ORDER BY event)"

$CLICKHOUSE_LOCAL --query "
$SETTINGS
SET collect_hash_table_stats_during_aggregation = 1, adaptive_aggregator_disable_thaw = 0;

$FULL FORMAT Null;
CREATE TEMPORARY TABLE a1 ENGINE = Memory AS $COUNTERS;
$FULL FORMAT Null;
CREATE TEMPORARY TABLE a2 ENGINE = Memory AS $COUNTERS;
$TOPK FORMAT Null;
CREATE TEMPORARY TABLE a3 ENGINE = Memory AS $COUNTERS;

SELECT 'Verdict not read by top-K',
    (SELECT * FROM a1)[1] > 0 AND (SELECT * FROM a1)[2] > 0,
    (SELECT * FROM a2) = (SELECT * FROM a1),
    (SELECT * FROM a3)[1] > (SELECT * FROM a2)[1];
"

$CLICKHOUSE_LOCAL --query "
$SETTINGS
SET collect_hash_table_stats_during_aggregation = 1, adaptive_aggregator_disable_thaw = 0;

$TOPK FORMAT Null;
CREATE TEMPORARY TABLE b1 ENGINE = Memory AS $COUNTERS;
$FULL FORMAT Null;
CREATE TEMPORARY TABLE b2 ENGINE = Memory AS $COUNTERS;

SELECT 'Verdict not recorded by top-K',
    (SELECT * FROM b1)[1] > 0 AND (SELECT * FROM b1)[2] > 0,
    (SELECT * FROM b2)[1] > (SELECT * FROM b1)[1];
"
