#!/usr/bin/env bash

# `SpillingHashJoin` decides whether a deferred build fits from its projection of the bytes the build will take. The
# build logs that projection next to the bytes it took after the inserts. The projection must not be lower than the
# bytes taken for these shapes: unique and duplicate keys, string keys, long `LowCardinality` values, keys spread over
# 96 slots with one duplicate each, and a key with 90% of the rows. For keys spread evenly, no bucket outgrows its
# reserve.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="join_algorithm = 'parallel_hash', enable_hash_join_deferred_build = 1, collect_hash_table_stats_during_joins = 0,
    max_bytes_before_external_join = 10737418240, max_bytes_ratio_before_external_join = 0, query_plan_join_swap_table = 'false',
    enable_parallel_replicas = 0"

check()
{
    local name=$1 threads=$2 threshold=$3 right=$4 kind=$5
    local logs
    logs=$($CLICKHOUSE_CLIENT --send_logs_level=debug --query "
        SELECT count() FROM (SELECT k FROM (${right}) LIMIT 1) l ${kind} JOIN (${right}) r ON l.k = r.k
        SETTINGS ${SETTINGS}, max_threads = ${threads}, parallel_hash_join_threshold = ${threshold} FORMAT Null" 2>&1)
    local line
    line=$(echo "$logs" | grep -o -E 'Deferred build: projected [0-9]+ bytes, built [0-9]+ bytes' | head -1)
    local projected built
    projected=$(echo "$line" | awk '{print $4}')
    built=$(echo "$line" | awk '{print $7}')
    if [ -n "$projected" ] && [ "$projected" -ge "$built" ]; then echo "$name projected >= built"; else echo "$name $line"; fi
}

# The left side is one row: only the build matters.
check unique_u64 4 0 "SELECT number AS k FROM numbers(1000000)" INNER
check duplicates_u64_all 4 0 "SELECT intDiv(number, 8) AS k FROM numbers(1000000)" INNER
check duplicates_u64_serial 4 18446744073709551615 "SELECT intDiv(number, 8) AS k FROM numbers(1000000)" INNER
check unique_string 4 0 "SELECT toString(number) AS k FROM numbers(300000)" INNER
check long_low_cardinality 1 18446744073709551615 "SELECT toLowCardinality(concat(repeat('x', 200), toString(number % 5000))) AS k FROM numbers(200000)" INNER
check long_low_cardinality_nullable 1 18446744073709551615 "SELECT toLowCardinality(toNullable(concat(repeat('x', 200), toString(number % 5000)))) AS k FROM numbers(200000)" INNER
check sparse_duplicates_96_slots 96 0 "SELECT intDiv(number, 2) AS k FROM numbers(2000000)" INNER
check hot_key 4 0 "SELECT if(number < 900000, 0, number) AS k FROM numbers(1000000)" INNER
check right_join_nullable 4 0 "SELECT if(number % 3 = 0, NULL, number) AS k FROM numbers(1000000)" RIGHT

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM (SELECT 1 AS k) l INNER JOIN (SELECT number AS k FROM numbers(1000000)) r ON l.k = r.k
    SETTINGS ${SETTINGS}, max_threads = 4, parallel_hash_join_threshold = 0, log_comment = '05413_uniform' FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'uniform buckets grown', ProfileEvents['HashJoinDeferredBuildBucketsGrown']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05413_uniform'"
