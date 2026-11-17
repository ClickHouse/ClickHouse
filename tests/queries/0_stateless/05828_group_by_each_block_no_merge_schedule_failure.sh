#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `group_by_each_block_no_merge` every input block is converted to chunks on its own, and then its
# aggregation state is reset. A two-level table of more than 100000 groups is converted by a thread pool.
# If scheduling the first conversion task fails, the query must fail instead of silently dropping the
# groups of the block. The failpoint is enabled in a separate `clickhouse local`, so it affects no other test.

settings="group_by_each_block_no_merge = 1, max_threads = 4, max_threads_min_free_memory_per_thread = 0,
    max_block_size = 200000, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0,
    enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0"
# `numbers_mt` gives several streams, so the two-level threshold is not reset to 0 for a single stream.
query="SELECT count(), sum(s) FROM (SELECT number AS k, sum(number) AS s FROM numbers_mt(400000) GROUP BY k) SETTINGS $settings"

${CLICKHOUSE_LOCAL} --query "$query"
${CLICKHOUSE_LOCAL} --query "SYSTEM ENABLE FAILPOINT aggregator_convert_two_level_schedule_throw; $query" 2>&1 \
    | grep -o -m1 'FAULT_INJECTED'
