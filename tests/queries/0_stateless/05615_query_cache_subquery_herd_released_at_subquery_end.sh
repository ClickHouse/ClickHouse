#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The herd token of a subquery in the Planner-level (`is_subquery = 1`) query result cache must be released as soon as the
# subquery itself finishes, not when the enclosing query finishes. Otherwise a fast query sharing a fast subquery with a slow
# query would wait for the whole slow query before it could even finish planning.

# The subquery is unique to this test so that concurrently running tests don't share its cache entry.
SUBQUERY="SELECT sum(number) AS x FROM numbers(1000) WHERE toString(number) != '${CLICKHOUSE_DATABASE}'"
SETTINGS="enable_analyzer=1, use_query_cache=1, query_cache_for_subqueries=1, query_cache_min_query_runs=0, query_cache_min_query_duration=0, query_cache_herd_wait_timeout=300, function_sleep_max_microseconds_per_block=30000000"
SLOW_QUERY_ID="qrc_subquery_herd_slow_${CLICKHOUSE_DATABASE}"

# The slow query computes the subquery quickly (it is the build side of the join), but then spends about 10 seconds sleeping on
# the rows of the other side.
${CLICKHOUSE_CLIENT} --query_id "${SLOW_QUERY_ID}" --query "
SELECT s.x, sleepEachRow(0.5) FROM numbers(20) AS n, (${SUBQUERY}) AS s
SETTINGS ${SETTINGS}
" >/dev/null 2>&1 &

for _ in $(seq 1 300); do
    running=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '${SLOW_QUERY_ID}'")
    [ "${running}" -ge 1 ] && break
    sleep 0.1
done

# The fast query shares the subquery (in the same position, so that the subquery has the same cache key). It either waits for the
# slow query to compute the subquery or reads its result from the cache - in both cases it finishes while the slow query is still
# running.
${CLICKHOUSE_CLIENT} --query "SELECT 2, s.x FROM numbers(1) AS n, (${SUBQUERY}) AS s SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '${SLOW_QUERY_ID}'"

# The subquery result is in the cache already, while the enclosing slow query is still running.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%${CLICKHOUSE_DATABASE}%'"

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${SLOW_QUERY_ID}' SYNC" >/dev/null
wait
