#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: pauses a server-global failpoint, which a concurrent request could take.
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

# An annotation probe of a request to /api/v1/query runs as a query with its own id. It stops when the request
# is killed by its query id, and when the request runs out of its execution time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="prometheus_annotation_probe_pause_before_pull"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
SET allow_experimental_time_series_table = 1;
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples)
SELECT 'foo', map('job', 'a'), arrayMap(i -> (toDateTime64(1700000000 + 15 * i, 3), toFloat64(i)), range(41));
"

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/prometheus/api/v1"
OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"

# `rate(foo[5m])` runs a probe for its annotation, which pauses at the failpoint.
function request()
{
    ${CLICKHOUSE_CURL} -sS --get "${BASE}/query" --data-urlencode "query=rate(foo[5m])" --data-urlencode "time=1700000600" \
        --data-urlencode "database=${CLICKHOUSE_DATABASE}" --data-urlencode "table=ts" \
        --data-urlencode "enable_time_series_aggregate_functions=1" "$@" > "$OUT"
}

# Prints the status of the response, whether its error has the expected text, and the type and the exception code
# of the probe in the query log.
function print_result()
{
    local log_comment=$1
    local error_text=$2
    python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
print(d["status"], sys.argv[2] in d.get("error", ""))' "$OUT" "$error_text"
    for _ in {1..60}; do
        result=$($CLICKHOUSE_CLIENT -q "
SYSTEM FLUSH LOGS query_log;
SELECT type, exception_code FROM system.query_log
WHERE (current_database = currentDatabase() OR query LIKE '%${CLICKHOUSE_DATABASE}%') AND type != 'QueryStart'
    AND log_comment = '${log_comment}' AND query LIKE '%arrayExists%' AND query_id != '${log_comment}'")
        [ -n "$result" ] && break
        sleep 0.5
    done
    echo "$result"
}

echo "--- KILL QUERY of the request stops its running probe"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
request -H "X-ClickHouse-Query-Id: ${query_id}" --data-urlencode "log_comment=${query_id}" &
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${query_id}' ASYNC FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait
print_result "$query_id" "Query was cancelled"

echo "--- the execution time limit of the request stops its running probe"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_timeout"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
request -H "X-ClickHouse-Query-Id: ${query_id}" --data-urlencode "log_comment=${query_id}" --data-urlencode "max_execution_time=1" &
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"
sleep 1.5
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
wait
print_result "$query_id" "Timeout exceeded"

rm -f "$OUT"
$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
