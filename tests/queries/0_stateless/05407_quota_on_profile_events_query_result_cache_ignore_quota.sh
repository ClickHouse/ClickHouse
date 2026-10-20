#!/usr/bin/env bash
# A query exempt from quotas (here: a `SELECT` from `system.one`) stays exempt when its result is read from the
# query result cache, which runs no interpreter to tell: it is neither rejected nor charged by quotas over profile events.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05407_${CLICKHOUSE_DATABASE}"
Q="quota_05407_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX QueryCacheHits = 1 TO ${U}"

CACHED_QUERY="SELECT dummy FROM system.one SETTINGS use_query_cache = 1, query_cache_system_table_handling = 'save', query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 600, query_cache_tag = '${CLICKHOUSE_DATABASE}'"

# The first run populates the cache, the others read from it.
for _ in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}"
done
${CLICKHOUSE_CLIENT} -q "SELECT profile_events['QueryCacheHits'] FROM system.quotas_usage WHERE quota_name = '${Q}'"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "SELECT query_cache_usage, count() FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase() AND user = '${U}' AND type = 'QueryFinish' AND query LIKE '%use_query_cache%' GROUP BY ALL ORDER BY ALL"

cleanup
