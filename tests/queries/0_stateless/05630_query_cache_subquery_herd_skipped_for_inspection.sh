#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A plan built only to be inspected (`EXPLAIN`, `EXPLAIN PIPELINE`, `DESCRIBE (SELECT ...)`) is never executed, so it must
# not wait for a concurrent execution of the identical cached subquery ("thundering herd" avoidance).

EXECUTOR_QUERY_ID="05630_executor_${CLICKHOUSE_DATABASE}"

# `EXPLAIN` sets these two settings itself, so set them for the subquery explicitly to get the same cache key.
SETTINGS="enable_analyzer = 1, use_query_cache = 1, query_cache_for_subqueries = 1, \
    query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, \
    query_cache_nondeterministic_function_handling = 'save', query_cache_herd_wait_timeout = 60, \
    use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0, \
    max_block_size = 1, max_execution_time = 600"

SUBQUERY="SELECT sum(sleepEachRow(1)) AS x FROM numbers(120)"

function query_is_running()
{
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$1'")" -ge 1 ]
}

# The inspected statements finish in a time which is far below the duration of the executor (120 seconds) and the herd
# wait timeout (60 seconds).
function check_prompt()
{
    local name=$1
    local query=$2
    local start=${SECONDS}
    ${CLICKHOUSE_CLIENT} --query "${query}" >/dev/null 2>&1
    local elapsed=$((SECONDS - start))
    if [ "${elapsed}" -lt 30 ]; then echo "${name}: OK"; else echo "${name}: took ${elapsed} seconds"; fi
}

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "SELECT 0, x FROM (${SUBQUERY}) SETTINGS ${SETTINGS}" --query_id "${EXECUTOR_QUERY_ID}" >/dev/null 2>&1 &
executor_pid=$!

while ! query_is_running "${EXECUTOR_QUERY_ID}"; do
    sleep 0.1
done

# The executor owns the herd token of the subquery from now on, until the subquery finishes.
check_prompt "EXPLAIN" "EXPLAIN SELECT 1, x FROM (${SUBQUERY}) SETTINGS ${SETTINGS}"
check_prompt "EXPLAIN PIPELINE" "EXPLAIN PIPELINE SELECT 2, x FROM (${SUBQUERY}) SETTINGS ${SETTINGS}"
check_prompt "DESCRIBE" "DESCRIBE (SELECT 3, x FROM (${SUBQUERY}) SETTINGS ${SETTINGS})"

query_is_running "${EXECUTOR_QUERY_ID}" && echo "executor is still running"

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${EXECUTOR_QUERY_ID}' SYNC" >/dev/null
wait "${executor_pid}"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
