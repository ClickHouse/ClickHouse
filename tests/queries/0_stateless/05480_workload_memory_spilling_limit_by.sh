#!/usr/bin/env bash
# Tags: long, no-parallel
# The singleton `MEMORY RESERVATION` resource is created and dropped by these tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >& /dev/null || :
    $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS memory" >& /dev/null || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE WORKLOAD $workload SETTINGS max_memory = '2Gi', max_memory_before_spill = '16Mi';
"

# Only workload pressure can trigger spilling: the per-query thresholds and the adaptive scheduler are disabled.
settings=(
    --workload "$workload"
    --max_bytes_before_external_limit_by 0
    --max_bytes_ratio_before_external_limit_by 0
    --max_bytes_before_external_sort 0
    --max_bytes_ratio_before_external_sort 0
    --enable_adaptive_memory_spill_scheduler 0
    --enable_parallel_replicas 0
    --max_threads 1
    --max_block_size 32768
    --max_untracked_memory 0
    --min_bytes_to_spill 10Mi
    --prefer_external_sort_block_bytes 10Mi
)

function run_query()
{
    $CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME/$1" -q "$2"
}

# Repeated keys cross the transition from hashing to spill runs, whose merge keeps two rows of each key.
run_query typed "
SELECT count() = 2097152, sum(k) = 1099510579200
FROM (SELECT number % 1048576 AS k FROM numbers(6291456) LIMIT 2 BY k)
"

# Generic keys are compared through fingerprints in the spill runs, and the offset skips the first two rows of
# each key across them.
run_query offset "
SELECT count() = 1048576, sum(k[1]) = 549755289600
FROM (SELECT [number % 1048576] AS k FROM numbers(4194304) LIMIT 1 OFFSET 2 BY k)
"

# The `LIMIT BY` follows a sort, so after spilling it must still keep the first two rows of each key in the sorted
# order.
run_query ordered "
SELECT count() = 2097152, sum(v) = 6597068718080, groupArray(v) = arrayReverseSort(groupArray(v))
FROM (SELECT number % 1048576 AS k, number AS v FROM numbers(4194304) ORDER BY v DESC LIMIT 2 BY k)
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT splitByChar('/', log_comment)[-1], count() = 1,
       max(ProfileEvents['MemoryReservationSpilledBytes']) > 0,
       max(ProfileEvents['ExternalLimitByUncompressedBytes']) > 0,
       max(ProfileEvents['ExternalLimitByMerge']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND event_date >= yesterday()
    AND startsWith(log_comment, '$CLICKHOUSE_TEST_UNIQUE_NAME/')
    AND type = 'QueryFinish'
    AND is_initial_query
GROUP BY log_comment
ORDER BY log_comment
"
