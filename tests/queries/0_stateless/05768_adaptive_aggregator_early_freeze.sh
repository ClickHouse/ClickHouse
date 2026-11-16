#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A learning table of the adaptive aggregator freezes before `adaptive_aggregator_freeze_threshold` keys when the
# first 4,096 rows it takes, its trial, show that its keys do not repeat, but only while its producer expects enough
# more rows for the freeze to pay: 16 rows per partition of the staging layout, which is 8,192 rows with the 512
# partitions of two producers. Each cell aggregates two streams of `rows` rows, one per producer, with blocks of
# 4,096 rows, so a block ends where the trial does.
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory, so each stream has a
# table of its own. The thresholds are pinned because the runner randomizes them, and every cell runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
cell()
{
    local rows=$1 key=$2 fact=$3
    $CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 4096;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT count(), sum(c) FROM (
    SELECT ${key} AS k, count() AS c
    FROM (SELECT number FROM numbers(${rows}) UNION ALL SELECT number + 10000000 FROM numbers(${rows}))
    GROUP BY k);
WITH
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') AS freezes,
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') AS staged
SELECT ${fact};
"
}

# Keys that never repeat, with 200,000 rows per stream: each table freezes at the end of its trial, or one block
# later, so it stages all but at most 8,192 of its rows. A table that learned up to the threshold of 16,384 keys
# would stage at most 183,616 of them. The streams are long so that the freeze pays however far the reading runs
# ahead of the aggregation: the rows expected are estimated from the share of the input read so far, and with
# 400,000 rows to read the estimate still passes the 12,288 rows that a producer needs when 30 blocks are read
# and not yet aggregated.
cell 200000 'number' "'tables froze after the trial', freezes = 2 AND staged >= 2 * (200000 - 8192)"

# The same keys with 5,000 rows per stream: a producer that took its trial expects at most the 10,000 rows of both
# streams, fewer than the 4,096 it received plus 8,192, so no table freezes, and none reaches the threshold either.
cell 5000 'number' "'tables did not freeze', freezes = 0 AND staged = 0"

# Every key twice in a row, with 36,864 rows per stream: half of the rows of the trial repeat a key, so each table
# learns up to the threshold of 16,384 keys, which takes 32,768 rows, and stages at most a record for each of the
# 4,096 rows that follow. A table frozen at its trial would stage the 16,384 keys of 32,768 rows.
cell 36864 'intDiv(number, 2)' "'tables froze at the threshold', freezes = 2 AND staged > 0 AND staged <= 2 * 4096"
