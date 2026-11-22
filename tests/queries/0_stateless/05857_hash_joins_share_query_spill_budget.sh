#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan
# no sanitizers -- memory consumption is unpredictable with sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Several hash joins, each under its own spill threshold, whose sum reaches the budget of the query
# (`max_bytes_ratio_before_external_join` of the memory available to the user): the joins share it and spill.
# A dedicated user, because the ratio is taken of the memory limits above the query.

USER="u05857_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS max_memory_usage_for_user = '1Gi'"
$CLICKHOUSE_CLIENT -q "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"

for i in 0 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t${i} (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t${i} SELECT number, number + ${i} FROM numbers(1000000)"
done

SETTINGS="
    max_threads = 2,
    max_bytes_before_external_join = 0,
    max_bytes_ratio_before_external_join = 0.2,
    max_bytes_in_join = 0,
    enable_adaptive_memory_spill_scheduler = 0,
    enable_parallel_replicas = 0,
    query_plan_join_swap_table = 0,
    query_plan_optimize_join_order_limit = 0,
    collect_hash_table_stats_during_joins = 1,
    grace_hash_join_initial_buckets = 1"

ONE_JOIN="SELECT count(), sum(t1.v) FROM t0 JOIN t1 ON t0.k = t1.k"
SIX_JOINS="SELECT count(), sum(t1.v + t2.v + t3.v + t4.v + t5.v + t6.v) FROM t0
    JOIN t1 ON t0.k = t1.k JOIN t2 ON t0.k = t2.k JOIN t3 ON t0.k = t3.k
    JOIN t4 ON t0.k = t4.k JOIN t5 ON t0.k = t5.k JOIN t6 ON t0.k = t6.k"

function run()
{
    local query=$1 algorithm=$2 comment=$3
    $CLICKHOUSE_CLIENT --user "${USER}" -q "$query SETTINGS join_algorithm = '$algorithm', log_comment = '05857_${comment}_${CLICKHOUSE_DATABASE}', $SETTINGS${4:+, $4}"
}

run "$ONE_JOIN" hash one_hash
run "$SIX_JOINS" hash six_hash_cold
# The second run reserves its hash tables from the statistics the first one cached.
run "$SIX_JOINS" hash six_hash_warm
run "$ONE_JOIN" grace_hash one_grace
run "$SIX_JOINS" grace_hash six_grace
# Each right table arrives as one block, so only the check after the insert can hold it to its share.
run "$SIX_JOINS" grace_hash six_one_block_grace "max_threads = 1, max_block_size = 2000000, preferred_block_size_bytes = 0"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(replaceOne(log_comment, '05857_', ''), '_${CLICKHOUSE_DATABASE}', '') AS name,
        -- \`hash\` spills by switching to \`GraceHashJoin\`, \`grace_hash\` by writing buckets to disk.
        if(name LIKE '%grace', ProfileEvents['ExternalJoinUncompressedBytes'], ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']) > 0 AS spilled
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND log_comment LIKE '05857\\_%\\_${CLICKHOUSE_DATABASE}'
        AND type = 'QueryFinish'
        AND event_date >= yesterday()
    ORDER BY event_time_microseconds
"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
