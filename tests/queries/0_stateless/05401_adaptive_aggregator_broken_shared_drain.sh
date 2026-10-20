#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A pressure sweep that throws while draining into the adaptive session's shared table, for example
# on a memory limit hit while creating the states of a new key, leaves that key in the table with a
# null state. The other producers kept running until the executor cancelled them, and the next one
# to take the coordinator lock dereferenced the null state: a drain adding rows into it, or a residue
# release writing the table to disk through the serialization of `topK`. The failed drain now
# cancels the session before it releases the coordinator lock, so the query fails with the drain's
# exception.
#
# The failpoint fails the creation of the states of the first key a drain inserts. With a threshold
# of 200 MB the sweeps stay in the tail regime, which drains into the shared table, while the other
# producers queue on the coordinator lock. The race is narrow - without the fix about one run in
# seventy crashed, and only with 64 threads - so this exercises the path rather than reproducing the
# crash reliably. The outer query reads the aggregates, so they are not pruned from the inner one.

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
