#!/usr/bin/env bash
# Tags: no-parallel
# The singleton `MEMORY RESERVATION` resource is created and dropped by this test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A spill requested by the memory scheduler runs outside of the processor's `consume`, while
# another transform of the same aggregation may be merging the hash tables into a query result
# preview snapshot. The spill must take part in the same locking, and it stops the previews: the
# in-memory state is incomplete afterwards. The result must be the same as without previews.

workload=w_$CLICKHOUSE_TEST_UNIQUE_NAME

function cleanup()
{
  $CLICKHOUSE_CLIENT -q "DROP WORKLOAD IF EXISTS $workload" >& /dev/null || :
  $CLICKHOUSE_CLIENT -q "DROP RESOURCE IF EXISTS memory" >& /dev/null || :
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -nm -q "
CREATE OR REPLACE RESOURCE memory (MEMORY RESERVATION);
CREATE OR REPLACE WORKLOAD $workload SETTINGS max_memory = '4Gi', max_memory_before_spill = '100Mi';
"

# The per-operator thresholds are disabled, so the only spill trigger is the workload soft limit.
# The hash tables stay single-level until they spill, so the previews are active until then.
SETTINGS="workload=$workload&max_rows_to_read=0&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0"
SETTINGS="${SETTINGS}&enable_adaptive_aggregator=0&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
SETTINGS="${SETTINGS}&min_bytes_to_spill=8388608&max_threads=4&max_block_size=65536"
SETTINGS="${SETTINGS}&query_result_previews=1&query_result_previews_min_interval_ms=10"
SETTINGS="${SETTINGS}&query_result_previews_max_result_rows=0&query_result_previews_max_result_bytes=0"
URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&log_comment=${CLICKHOUSE_TEST_UNIQUE_NAME}&${SETTINGS}"

# The aggregation feeds the result directly (through the sorting), so its previews are active.
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "
SELECT number AS k, count() AS c FROM numbers_mt(10000000) GROUP BY k ORDER BY c DESC, k DESC LIMIT 3
FORMAT JSONCompactEachRow")

if [ "$(echo "${RESPONSE}" | grep -c '"packet":"preview"')" -ge 1 ]; then echo "has previews"; else echo "no previews: ${RESPONSE}"; fi
# The result may be split into several `data` packets; their payloads concatenated are the result.
echo "${RESPONSE}" | grep '"packet":"data"' | sed -e 's/^{"packet":"data","data":"//' -e 's/"}$//' | tr -d '\n' | sed -e 's/\\n/\n/g'

# The `query_log` entry is written after the HTTP response is sent, so retry until it appears.
for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    RESULT=$($CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['MemoryReservationSpilledBytes'] > 0, ProfileEvents['ExternalAggregationWritePart'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND event_date >= yesterday()
        AND log_comment = '$CLICKHOUSE_TEST_UNIQUE_NAME'
        AND type = 'QueryFinish'
    ")
    [ -n "$RESULT" ] && break
    sleep 0.5
done
echo "$RESULT"
