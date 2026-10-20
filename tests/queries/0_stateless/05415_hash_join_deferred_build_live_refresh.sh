#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# The fail point is server-wide.

# The live projection of a deferred build merges the sketches of the build streams while they insert into them, and
# `SpillingHashJoin` may switch to `GraceHashJoin` while another stream reads the projection. The fail point makes every
# block recompute the projection, so these readers and writers meet as often as possible. The thread sanitizer would
# report a data race between them. Every query must give the result of the streaming build, with and without a switch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="join_algorithm = 'parallel_hash', collect_hash_table_stats_during_joins = 0, max_bytes_ratio_before_external_join = 0,
    parallel_hash_join_threshold = 0, grace_hash_join_initial_buckets = 4, query_plan_join_swap_table = 'false',
    enable_parallel_replicas = 0, max_block_size = 8192"
QUERY="SELECT count(), sum(cityHash64(l.k, r.v))
    FROM (SELECT number * 3 AS k FROM numbers(100000)) l
    INNER JOIN (SELECT intDiv(number, 2) AS k, number AS v FROM numbers_mt(2000000)) r ON l.k = r.k"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT hash_join_deferred_build_refresh_every_block"
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT hash_join_deferred_build_refresh_every_block"' EXIT

echo "streaming $($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS ${SETTINGS}, enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0, max_threads = 16")"
for threads in 16 96; do
    # A threshold far above the build keeps `SpillingHashJoin` without a switch.
    for budget in 1099511627776 16777216; do
        echo "threads $threads budget $budget $($CLICKHOUSE_CLIENT --query "$QUERY SETTINGS ${SETTINGS}, enable_hash_join_deferred_build = 1,
            max_bytes_before_external_join = $budget, max_threads = $threads")"
    done
done
