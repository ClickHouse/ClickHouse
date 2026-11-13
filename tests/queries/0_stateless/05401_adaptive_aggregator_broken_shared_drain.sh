#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The creation of the states of a key that the adaptive merge inserts into its destination fails, as
# on a memory limit, and leaves that key in the destination with a null state. The query must fail
# with that exception rather than touch the null state: aggregate rows into it, write it to disk or
# serialize it through `topK`. This test was first written for the shared drain table of the earlier
# adaptive aggregator, where a failed drain left null states that other producers then used.
#
# The failpoint fails the creation of the states of the first key the merge inserts. The tables freeze
# at 1000 keys, so the 200000 keys reach the merge as staged records. The outer query reads the
# aggregates, so they are not pruned from the inner one.

$CLICKHOUSE_LOCAL --query "
SYSTEM ENABLE FAILPOINT adaptive_aggregation_drain_create_states_throw;
SET max_threads = 16;
SET max_block_size = 8192;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 1000;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 1000;
SET group_by_two_level_threshold_bytes = 1000000;
SET max_bytes_before_external_group_by = 200000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT count(), sum(length(t)), sum(u) FROM
(
    SELECT toString(number % 200000) AS k, topK(1, 100)(toString(number)) AS t, uniqExact(number) AS u
    FROM numbers_mt(5000000)
    GROUP BY k
);
" 2>&1 | grep -o -m1 'FAULT_INJECTED'
