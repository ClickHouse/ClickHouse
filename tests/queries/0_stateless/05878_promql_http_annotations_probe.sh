#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

# The query that /api/v1/query runs to find the metric names for the `infos` array (the annotation probe)
# is a helper of the request: limits, quotas and result-shaping settings of the request do not apply to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05616_promql_http_annotations.lib
. "$CUR_DIR"/05616_promql_http_annotations.lib

create_annotations_table

TEST_USER="user_${CLICKHOUSE_DATABASE}"

echo "--- the annotation probe is a helper of the query: public result limits and quotas do not apply to it"
# The probe of this query returns the two non-counter metric names (foo and total_requests), so it is limited
# by max_result_rows=1 if it is not exempt from the request's public result limits.
instant $T 'count(rate({__name__=~"foo|total_requests"}[5m]))' --data-urlencode 'max_result_rows=1'
# The probe is a separate query if it is not exempt from the quota; with a quota of one query the
# request succeeds only if the probe is not charged against the user's quota.
$CLICKHOUSE_CLIENT -q "
CREATE USER ${TEST_USER};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER};
GRANT CREATE TEMPORARY TABLE ON *.* TO ${TEST_USER};
CREATE QUOTA ${TEST_USER} FOR INTERVAL 1 hour MAX queries = 1 TO ${TEST_USER};
"
CURL_AUTH="-u ${TEST_USER}:"
instant $T 'rate(foo[5m])'

# The quota of the user is exhausted now, so the request is rejected before an annotation probe reads data.
# A probe has the log comment of its request and its own query id.
rejected="${CLICKHOUSE_DATABASE}_rejected"
instant $T 'rate(foo[5m])' -H "X-ClickHouse-Query-Id: ${rejected}" --data-urlencode "log_comment=${rejected}"
# The rejected query and the start of any probe are logged before the response is sent, so one flush is enough.
result=$($CLICKHOUSE_CLIENT -q "
SYSTEM FLUSH LOGS query_log;
SELECT countIf(query_id != '${rejected}')
FROM system.query_log WHERE (current_database = currentDatabase() OR query LIKE '%${CLICKHOUSE_DATABASE}%')
    AND log_comment = '${rejected}'
HAVING countIf(query_id = '${rejected}' AND type = 'ExceptionBeforeStart' AND exception_code = 201) > 0")
echo "probes run by the rejected request: $result"
CURL_AUTH=
$CLICKHOUSE_CLIENT -q "
DROP QUOTA ${TEST_USER};
DROP USER ${TEST_USER};
"

echo "--- the settings shaping the public result do not apply to the annotation probe"
instant $T 'rate(foo[5m])' --data-urlencode 'filter=value > 0'
instant $T 'rate(foo[5m])' --data-urlencode 'order=value'
instant $T 'rate(foo[5m])' --data-urlencode 'sort=-value' --data-urlencode 'limit=1' --data-urlencode 'offset=1'

echo "--- the annotation probes keep the execution time limit of the request, which is checked on the request's status"
instant $T 'rate(foo[5m]) + rate(foo[10m])' --data-urlencode 'max_execution_time=1234' --data-urlencode "log_comment=${CLICKHOUSE_DATABASE}_time_limit"
# The query log entries are written after the HTTP response is sent, so retry until they appear.
for _ in {1..60}; do
    result=$($CLICKHOUSE_CLIENT -q "
SYSTEM FLUSH LOGS query_log;
SELECT countIf(query LIKE '%arrayExists%') > 0, countIf(Settings['max_execution_time'] != '1234')
FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '${CLICKHOUSE_DATABASE}_time_limit'")
    [[ $result == 1* ]] && break
    sleep 0.5
done
echo "$result"

echo "--- the annotation probe's query result cache write is finalized"
# The probe's SELECT is eligible for the query result cache. If its write is not finalized, the
# pending entry is discarded when the probe's pipeline is destroyed and the entry never appears.
# The execution time limit doesn't change the cache key of the probe between the requests.
instant $T 'rate(foo[5m])' --data-urlencode 'use_query_cache=1' --data-urlencode 'query_cache_nondeterministic_function_handling=save' --data-urlencode 'max_execution_time=60'
$CLICKHOUSE_CLIENT -q "SELECT count() AS probe_cache_entries FROM system.query_cache WHERE query LIKE '%${CLICKHOUSE_DATABASE}%' AND query LIKE '%timeSeriesExtractTag%' AND query LIKE '%arrayExists%'" | grep -o '[0-9]*'
instant $T 'rate(foo[5m])' --data-urlencode 'use_query_cache=1' --data-urlencode 'query_cache_nondeterministic_function_handling=save' --data-urlencode 'max_execution_time=60'
# The query log entry is written after the HTTP response is sent, so retry until it appears.
for _ in {1..60}; do
    usage=$($CLICKHOUSE_CLIENT -q "
SYSTEM FLUSH LOGS query_log;
SELECT query_cache_usage FROM system.query_log
WHERE (current_database = currentDatabase() OR query LIKE '%${CLICKHOUSE_DATABASE}%') AND type = 'QueryFinish'
    AND query LIKE '%timeSeriesExtractTag%' AND query LIKE '%arrayExists%' AND query NOT LIKE '%probe_cache_entries%'
ORDER BY event_time_microseconds DESC LIMIT 1")
    [ "$usage" = "Read" ] && break
    sleep 0.5
done
echo "$usage"

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
