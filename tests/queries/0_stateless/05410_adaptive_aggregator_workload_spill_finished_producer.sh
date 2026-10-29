#!/usr/bin/env bash
# Tags: no-parallel
# The singleton memory reservation resource is shared with the other workload tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME
resource=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.resources WHERE unit = 'MemoryByte'")
created_resource=
function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >/dev/null 2>&1 || :
    if [[ -n $created_resource ]]; then
        $CLICKHOUSE_CLIENT -q "DROP RESOURCE $resource" >/dev/null 2>&1 || :
    fi
}
trap cleanup EXIT

if [[ -z $resource ]]; then
    resource=memory_$CLICKHOUSE_TEST_UNIQUE_NAME
    $CLICKHOUSE_CLIENT -q "CREATE RESOURCE $resource (MEMORY RESERVATION)"
    created_resource=1
fi
$CLICKHOUSE_CLIENT -q "CREATE WORKLOAD $workload SETTINGS max_memory = '1Gi', max_memory_before_spill = 1"

# A producer of the adaptive aggregator that finishes hands its staged records over to the session, which keeps them
# spillable until the last producer finishes. Each lane of the `UNION ALL` feeds a producer of its own. The first four
# lanes are one block of distinct keys each, whose producer stages about 6 MiB of records and finishes at once. The
# workload is over its spill limit from the start, but it spills nothing below `min_bytes_to_spill`, so no producer
# spills its records itself. The last lane sleeps before its few rows arrive, so its producer is still running when the
# records of the other four, about 24 MiB together, are in the session. The query writes them to disk and reports the
# spill.
settings=(
    --workload "$workload"
    --enable_adaptive_aggregator 1
    --enable_adaptive_memory_spill_scheduler 0
    --adaptive_aggregator_freeze_threshold 0
    --adaptive_aggregator_freeze_threshold_bytes 0
    --adaptive_aggregator_disable_thaw 1
    --collect_hash_table_stats_during_aggregation 0
    --use_aggregation_memory_tracker 1
    --max_bytes_before_external_group_by 0
    --max_bytes_ratio_before_external_group_by 0
    --min_bytes_to_spill 8Mi
    --max_threads 2
    --max_threads_min_free_memory_per_thread 0
    --max_block_size 200000
    --max_untracked_memory 0
    --enable_parallel_replicas 0
    --log_queries 1
    --log_profile_events 1
)

$CLICKHOUSE_CLIENT "${settings[@]}" --log_comment "$CLICKHOUSE_TEST_UNIQUE_NAME" -q "
SELECT count(), sum(c) FROM (
    SELECT k, count() AS c FROM (
        SELECT number AS k FROM numbers(200000)
        UNION ALL SELECT 200000 + number AS k FROM numbers(200000)
        UNION ALL SELECT 400000 + number AS k FROM numbers(200000)
        UNION ALL SELECT 600000 + number AS k FROM numbers(200000)
        UNION ALL SELECT 800000 + number AS k FROM numbers(10) WHERE sleepEachRow(0.25) = 0)
    GROUP BY k)
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT ProfileEvents['AdaptiveAggregationStagedRecords'] > 0,
    ProfileEvents['AdaptiveAggregationSpilledRecords'] > 0,
    ProfileEvents['MemoryReservationSpilledBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME'
    AND type = 'QueryFinish' AND is_initial_query
"
