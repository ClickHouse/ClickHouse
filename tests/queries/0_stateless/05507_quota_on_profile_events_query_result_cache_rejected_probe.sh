#!/usr/bin/env bash
# A query rejected by an exhausted quota over `QueryCacheHits` / `QueryCacheMisses` does not consume
# another probe of the query result cache: the counters of the quota stay where they were.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05507_${CLICKHOUSE_DATABASE}"
Q="quota_05507_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05507"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05507 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05507 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05507 TO ${U}"

SETTINGS="SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 3600"
CACHED_QUERY="SELECT sum(a) FROM ${CLICKHOUSE_DATABASE}.t_05507 ${SETTINGS}"
# Differs from `CACHED_QUERY`, so it is a miss.
OTHER_QUERY="SELECT max(a) FROM ${CLICKHOUSE_DATABASE}.t_05507 ${SETTINGS}"

usage()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT profile_events['QueryCacheHits'], profile_events['QueryCacheMisses'] FROM system.quotas_usage WHERE quota_name = '${Q}'"
}

# Populates the cache before the quota exists.
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}"

${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q} FOR INTERVAL 100 year MAX QueryCacheHits = 1, QueryCacheMisses = 1 TO ${U}"

# Two hits exhaust `QueryCacheHits` (a quota is exceeded once the usage is above the maximum).
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}"
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}"
usage
# Another hit is rejected and is not charged.
${CLICKHOUSE_CLIENT} --user "${U}" -q "${CACHED_QUERY}" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
usage
# A miss is rejected as well and is not charged either.
${CLICKHOUSE_CLIENT} --user "${U}" -q "${OTHER_QUERY}" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
usage

cleanup
