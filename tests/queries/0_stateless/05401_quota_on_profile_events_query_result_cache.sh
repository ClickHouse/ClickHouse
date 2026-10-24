#!/usr/bin/env bash
# Quotas over profile events apply to results read from the query result cache: such a query is
# rejected once the quota is exhausted, and what it produces is charged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05401_${CLICKHOUSE_DATABASE}"
Q="quota_05401_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05401"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05401 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05401 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05401 TO ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX SelectQuery = 1, QueryCacheHits = 100 TO ${U}"

CACHED_QUERY="SELECT sum(a) FROM ${CLICKHOUSE_DATABASE}.t_05401 SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 3600"

# Populates the cache.
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectQuery'], profile_events['QueryCacheHits'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
# Reads the result from the cache and is charged (it does not run an interpreter, so it produces no `SelectQuery`).
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectQuery'], profile_events['QueryCacheHits'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
# Exhausts the quota with an uncached query.
${CLICKHOUSE_CLIENT} --user "${U}" -q "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_05401"
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['SelectQuery'], profile_events['QueryCacheHits'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
# A cache hit is rejected now as well.
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "SELECT query_cache_usage, count() FROM system.query_log WHERE current_database = currentDatabase() AND user = '${U}' AND type = 'QueryFinish' AND query LIKE '%use_query_cache%' GROUP BY ALL ORDER BY ALL"

cleanup
