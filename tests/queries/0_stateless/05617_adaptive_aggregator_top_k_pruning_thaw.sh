#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator thaws under the top-K pruning of `ORDER BY count() DESC LIMIT n`,
# and the pruning still holds: a producer that froze counts the groups of its own table into its bins at its
# finish. In each lane the first block holds 4096 distinct keys, which reach the freeze threshold. The next
# 150000 rows are the middle of the lane, whose rows miss the frozen table, all of them or half. Of the last
# 150000 rows, three in four carry the 10 keys of the top, which only the staged records or a thawed table
# hold, and the fourth carries 30 keys of a second tier, each with a ninth of the rows of a key of the top.
# Each lane is a single stream, so it feeds a producer of its own.
#
# With other aggregates, a repeating middle thaws the tables on the evidence alone, and
# `adaptive_aggregator_disable_thaw` keeps them frozen. A lone `count()` thaws only when the pruning is
# expected to free little of what it stages. A middle of 4000 keys spread over every block reaches the local
# top, so its tables thaw. A middle whose cold keys sit far below 10 hot keys keeps them frozen, whether the
# frozen table holds the hot keys or, after it stopped probing, the staged records do. With fewer local groups
# than `n`, every group may be in the top, so the same middle thaws. The bound of a bin at the merge sums the
# bins of all producers, so a producer counts a bin as pruned only below its top divided by their number.
# With 16 lanes, each with 10 keys of its own that are 12 times as frequent as the keys that all lanes share,
# the shared keys sit below an eighth of a lane's top but above a sixteenth, so the tables thaw. These lanes
# are shorter, 90112 rows in the middle and 16384 at the end, to keep the cell as light as the others.
#
# Every cell compares the rows of the top with those of the same query with the feature off, as a multiset:
# the top has no tie at its boundary. Reading every column keeps the other aggregate, whose records do not
# combine as those of a lone `count()` do. The cell also compares every returned row with the row of its key
# in the full aggregation. The last three columns say whether a table thawed and whether the merge skipped
# units and staged records. Each cell runs in its own clickhouse-local process, so the counters in
# `system.events` belong to it alone.
#
# The second tier keeps the pruning independent of how the merge threads are scheduled. The merge takes the
# buckets with the largest bins first and prunes by the smallest of the 10 best counts converted so far. The
# top alone would leave that threshold low until the last of its 10 keys is converted, which a slow thread
# can delay past every other bucket. With 40 keys, at most one bucket per thread is unfinished when a thread
# takes a bucket without them, so at least 10 of the 40 are converted by then, and the merge skips what the
# bucket holds below them: the keys of the first blocks and the less frequent keys of the middle.
#
# The last block keeps the hash-table statistics on. A top-K run whose tables thaw records the verdict, so the
# next run of the same query does not use the adaptive aggregator. The pruning and its limit are part of the
# key of the statistics, so the same aggregation without the `LIMIT`, or with another limit, has a verdict of
# its own: each of them uses the adaptive aggregator on its first run.

# Every query runs under a throw-mode `max_rows_to_group_by` far above the number of groups, as the server
# profile of the tests sets it: the top-K pruning and the thaw both hold under such a limit.
SETTINGS="SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 4096;
    SET max_rows_to_group_by = 10000000000, group_by_overflow_mode = 'throw';
    SET adaptive_aggregator_freeze_threshold = 4096, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET optimize_aggregation_in_order = 0, optimize_injective_functions_in_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;"

# The last rows of a lane: every fourth row goes to the second tier.
top='if(number % 4 = 3, 2100000 + intDiv(number, 4) % 30, 2000000 + intDiv(number, 4) % 10)'

function check()
{
    local label=$1 disable_thaw=$2 keys=$3 aggregates=$4
    local middle=${5:-'1000000 + toUInt64(sqrt(number % 10000))'} limit=${6:-10} lanes=${7:-4}
    local middle_rows=${8:-150000} top_rows=${9:-150000}
    local source="" lane
    for ((lane = 0; lane < lanes; ++lane))
    do
        source+="${source:+ UNION ALL }SELECT multiIf(number < 4096, number, number < 4096 + $middle_rows,
            ${middle//\{lane\}/$lane}, $top) AS k, number FROM numbers(4096 + $middle_rows + $top_rows)"
    done
    local query="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys ORDER BY c DESC LIMIT $limit"
    local full="SELECT $keys, $aggregates FROM ($source) GROUP BY $keys SETTINGS enable_adaptive_aggregator = 0"
    local columns
    columns=$(echo "$keys, $aggregates" | sed -E 's/[^,]* AS ([a-z]+)/\1/g')
    $CLICKHOUSE_LOCAL --query "
    $SETTINGS
    SET max_threads = $lanes, adaptive_aggregator_disable_thaw = $disable_thaw;

    SELECT '$label', $disable_thaw,
        (SELECT arraySort(groupArray(($columns))) FROM ($query))
            = (SELECT arraySort(groupArray(($columns))) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN ($full)) = 0;
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0,
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedRecords') > 0;
    " | paste -sd '\t'
}

uniform='1000000 + number % 4000'
cold_staged='if(number % 2 = 0, intDiv(number, 2) % 10, 1000000 + intDiv(number, 2) % 2000)'
hot_staged='if(number % 2 = 0, 3000000 + intDiv(number, 2) % 10, 1000000 + intDiv(number, 2) % 2000)'
# `{lane}` stands for the number of the lane.
own_hot='if(number % 34 = 0, 5000000 + {lane} * 100 + intDiv(number, 34) % 10, 1000000 + number % 4000)'

check 'String key, count and min' 0 'toString(k) AS sk' 'count() AS c, min(number) AS m'
check 'String key, count and min' 1 'toString(k) AS sk' 'count() AS c, min(number) AS m'
check 'Lone count, uniform staged keys' 0 'toString(k) AS sk' 'count() AS c' "$uniform"
check 'Lone count, cold staged keys' 0 'toString(k) AS sk' 'count() AS c' "$cold_staged"
check 'Lone count, hot keys staged' 0 'toString(k) AS sk' 'count() AS c' "$hot_staged"
check 'Lone count, fewer groups than the limit' 0 'toString(k) AS sk' 'count() AS c' "$cold_staged" 60000
check 'Lone count, hot keys of each lane' 0 'toString(k) AS sk' 'count() AS c' "$own_hot" 10 16 90112 16384

LANE="SELECT multiIf(number < 4096, number, number < 154096, 1000000 + toUInt64(sqrt(number % 10000)),
    $top) AS k, number FROM numbers(304096)"
FULL="SELECT toString(k) AS sk, count() AS c, min(number) AS m
    FROM ($LANE UNION ALL $LANE UNION ALL $LANE UNION ALL $LANE) GROUP BY sk"
# Each array is [local freezes, thaws], taken after each of five runs. The checks, in order: the first top-K
# run freezes and thaws; the second follows its verdict and does neither; the same aggregation without the
# `LIMIT` freezes and thaws; its second run follows its own verdict; a top-K run with another limit freezes.
COUNTERS="SELECT groupArray(value) FROM (SELECT event, coalesce(sum(value), 0) AS value FROM system.events
    WHERE event IN ('AdaptiveAggregationLocalFreezes', 'AdaptiveAggregationThaws') GROUP BY event ORDER BY event)"

$CLICKHOUSE_LOCAL --query "
$SETTINGS
SET collect_hash_table_stats_during_aggregation = 1, adaptive_aggregator_disable_thaw = 0;

$FULL ORDER BY c DESC LIMIT 10 FORMAT Null;
CREATE TEMPORARY TABLE c1 ENGINE = Memory AS $COUNTERS;
$FULL ORDER BY c DESC LIMIT 10 FORMAT Null;
CREATE TEMPORARY TABLE c2 ENGINE = Memory AS $COUNTERS;
$FULL FORMAT Null;
CREATE TEMPORARY TABLE c3 ENGINE = Memory AS $COUNTERS;
$FULL FORMAT Null;
CREATE TEMPORARY TABLE c4 ENGINE = Memory AS $COUNTERS;
$FULL ORDER BY c DESC LIMIT 20 FORMAT Null;
CREATE TEMPORARY TABLE c5 ENGINE = Memory AS $COUNTERS;

SELECT 'Verdict of top-K runs',
    (SELECT * FROM c1)[1] > 0 AND (SELECT * FROM c1)[2] > 0,
    (SELECT * FROM c2) = (SELECT * FROM c1),
    (SELECT * FROM c3)[1] > (SELECT * FROM c2)[1] AND (SELECT * FROM c3)[2] > (SELECT * FROM c2)[2],
    (SELECT * FROM c4) = (SELECT * FROM c3),
    (SELECT * FROM c5)[1] > (SELECT * FROM c4)[1];
"
