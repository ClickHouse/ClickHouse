#!/usr/bin/env bash

# A deferred build switches to `GraceHashJoin` at one of two points, and both give the result of the streaming build.
# Before it stores a block, when the live projection of its size reaches `max_bytes_before_external_join`: here many
# right blocks of keys alone, whose maps take far more than the blocks. Or once all blocks are stored, when the final
# projection reaches it: here a single right block, which no check before a block can see.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 0, max_bytes_ratio_before_external_join = 0,
    parallel_hash_join_threshold = 0, max_threads = 4, grace_hash_join_initial_buckets = 4, query_plan_join_swap_table = 'false',
    enable_parallel_replicas = 0"
LEFT="(SELECT number * 3 AS k FROM numbers(100000)) l"

run()
{
    local name=$1 query=$2 settings=$3 message=$4
    echo "$name streaming $($CLICKHOUSE_CLIENT --query "$query SETTINGS ${SETTINGS}, enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0")"
    local result
    result=$($CLICKHOUSE_CLIENT --send_logs_level=debug --query "$query SETTINGS ${SETTINGS}, enable_hash_join_deferred_build = 1, ${settings}" 2>"${CLICKHOUSE_TMP}/05414.log")
    echo "$name deferred $result"
    grep -q -F "$message" "${CLICKHOUSE_TMP}/05414.log" && echo "$name switched" || echo "$name did not switch where expected"
    rm -f "${CLICKHOUSE_TMP}/05414.log"
}

run "during the build" \
    "SELECT count(), sum(cityHash64(l.k)) FROM ${LEFT} INNER JOIN (SELECT number AS k FROM numbers(1000000)) r ON l.k = r.k" \
    "max_bytes_before_external_join = 8388608, max_block_size = 65536" \
    "reached the spill threshold while the right blocks arrive"

run "at the end of the build" \
    "SELECT count(), sum(cityHash64(l.k, r.v)) FROM ${LEFT} INNER JOIN (SELECT number AS k, number * 7 AS v FROM numbers(1000000)) r ON l.k = r.k" \
    "max_bytes_before_external_join = 25165824, max_block_size = 2000000" \
    "reached the spill threshold at the end of the build"
