#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator spills its staged records to one stream per partition, which all producers append to. The
# limit on temporary data fails a write to such a stream, and the producers that append to that stream afterwards must
# report the same `TOO_MANY_ROWS_OR_BYTES`, not `CANNOT_WRITE_AFTER_BUFFER_CANCELED`. The thresholds are pinned because
# the runner randomizes them, and each query is repeated because which producer fails first varies from run to run.
query="SELECT count(), sum(c) FROM (SELECT toUInt64(number % 100000) AS k, count() AS c FROM numbers_mt(400000) GROUP BY k);"
$CLICKHOUSE_LOCAL --ignore-error --query "
    SET max_threads = 16, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
    SET max_temporary_data_on_disk_size_for_query = 65536;
    $query $query $query $query $query
" 2>&1 | grep -oE "TOO_MANY_ROWS_OR_BYTES|CANNOT_WRITE_AFTER_BUFFER_CANCELED" | LC_ALL=C sort -u
