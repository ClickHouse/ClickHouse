#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Prevents running test in parallel, but multiple queries for the single test still run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Thundering herd for a `UNION` subquery that opts into the Planner-level query result cache with its own
# `SETTINGS use_query_cache = 1`: concurrent identical set operations must coalesce on one in-flight computation, just like
# concurrent identical plain subqueries do.
# The outer queries differ from each other, so the top-level (`is_subquery = 0`) cache cannot
# deduplicate them. only the herd of the set operation can. The first arm is not cached: only the last arm carries the clause.
# So coalescing on the last arm alone would still make every outer query scan the first arm. Without coalescing of the set
# operation every outer query scans the 10M rows itself; with it exactly one does and the others read the whole result of the
# set operation from the cache.
UNION_QUERY="SELECT sum(number) AS x FROM numbers(10000000) UNION ALL SELECT toUInt64(0) AS x FROM numbers(1) SETTINGS use_query_cache = 1"
SETTINGS="enable_analyzer=1, query_cache_min_query_runs=0, query_cache_min_query_duration=0, query_cache_herd_wait_timeout=300"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

{
    for i in $(seq 1 5); do
        ${CLICKHOUSE_CLIENT} --query "SELECT ${i}, x FROM (${UNION_QUERY}) ORDER BY x SETTINGS ${SETTINGS}" \
            --query_id "qrc_union_herd_${CLICKHOUSE_DATABASE}_${i}" &
    done
    wait
} | sort

for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query_id LIKE 'qrc_union_herd_${CLICKHOUSE_DATABASE}_%'
          AND type = 'QueryFinish'
    ")
    [ "${count}" -ge 5 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query "
SELECT countIf(read_rows > 1000000), countIf(read_rows <= 1000000)
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE 'qrc_union_herd_${CLICKHOUSE_DATABASE}_%'
  AND type = 'QueryFinish'
"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE is_subquery = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

SMALL_UNION_QUERY="SELECT number AS x FROM numbers(2) UNION ALL SELECT number FROM numbers(3) SETTINGS use_query_cache = 1"
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM (${SMALL_UNION_QUERY}) AS a, (${SMALL_UNION_QUERY}) AS b
SETTINGS ${SETTINGS}
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
