#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

threshold=134217728
log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log"
$CLICKHOUSE_LOCAL --send_logs_level=trace --query "
    SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers_mt(10000000) GROUP BY k)
    SETTINGS
        max_threads = 16,
        max_threads_min_free_memory_per_thread = 0,
        max_block_size = 65536,
        enable_adaptive_aggregator = 0,
        collect_hash_table_stats_during_aggregation = 0,
        group_by_two_level_threshold = 1,
        group_by_two_level_threshold_bytes = 1,
        max_bytes_before_external_group_by = ${threshold},
        max_bytes_ratio_before_external_group_by = 0
" 2> "$log"

grep -oE "Reserved [0-9.]+ [KMGT]?i?B on local disk" "$log" | $CLICKHOUSE_LOCAL --structure 'line String' --input-format LineAsString --query "
    SELECT 'reservations', count() > 0,
        'all below the threshold', max(parseReadableSize(extract(line, 'Reserved (.*) on local disk'))) < ${threshold}
    FROM table"
