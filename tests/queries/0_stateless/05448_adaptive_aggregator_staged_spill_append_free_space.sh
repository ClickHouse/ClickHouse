#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of the adaptive aggregator freezes within its first few thousand of the 100000 keys, so the records of the
# other keys are staged, and over the external-aggregation threshold they spill to one stream per partition, which the
# later spills append to. A spill checks the disk of the existing streams it appends to, once per disk. The failpoint
# makes that check ask for the largest possible size, as for a disk without room, so the query fails with
# `NOT_ENOUGH_SPACE` for a size that no stream creation asks for. The thresholds are pinned because the runner
# randomizes them, and clickhouse-local runs the query in a process of its own, so the failpoint affects no other test.
$CLICKHOUSE_LOCAL --ignore-error --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
    SYSTEM ENABLE FAILPOINT adaptive_aggregation_spill_no_room_to_append;
    SELECT count(), sum(c) FROM (SELECT toUInt64(number % 100000) AS k, count() AS c FROM numbers_mt(400000) GROUP BY k);
" 2>&1 | grep -oE "NOT_ENOUGH_SPACE|cannot reserve [0-9]+ bytes" | LC_ALL=C sort -u
