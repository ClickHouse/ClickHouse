#!/usr/bin/env bash
# A query rejected by an exhausted quota over `QueryCacheHits` / `QueryCacheMisses` does not consume
# another probe of the query result cache: the counters of the quota stay where they were.
# The same holds for what reading a cached result is charged with (`QueryCacheReadRows` / `QueryCacheReadBytes`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Quotas and users are server-global entities, so scope the names to this test's database.
U="user_05507_${CLICKHOUSE_DATABASE}"
Q="quota_05507_${CLICKHOUSE_DATABASE}"
U2="user2_05507_${CLICKHOUSE_DATABASE}"
Q2="quota2_05507_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q}"
    ${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${Q2}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U}"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${U2}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_05507"
}
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE USER ${U}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${U2}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_05507 (a UInt32) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05507 SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05507 TO ${U}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05507 TO ${U2}"

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

# A hit served to another user (the entries are not shared), whose quota limits the rows read from the cache.
# The other limits are not reached, they only make `system.quotas_usage` track these events.
# The cached result of `READ_QUERY` has 10 rows.
READ_QUERY="SELECT a FROM ${CLICKHOUSE_DATABASE}.t_05507 ORDER BY a ${SETTINGS} FORMAT Null"
read_usage()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT profile_events['QueryCacheHits'], profile_events['QueryCacheReadRows'], profile_events['QueryCacheReadBytes'] > 0 FROM system.quotas_usage WHERE quota_name = '${Q2}'"
}
echo "-- QueryCacheReadRows"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "${READ_QUERY}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${Q2} FOR INTERVAL 100 year MAX QueryCacheReadRows = 1, QueryCacheReadBytes = 1000000000, QueryCacheHits = 1000 TO ${U2}"
# The first hit is allowed and exhausts the quota.
${CLICKHOUSE_CLIENT} --user "${U2}" -q "${READ_QUERY}"
read_usage
# The following hits are rejected and do not read the result again.
${CLICKHOUSE_CLIENT} --user "${U2}" -q "${READ_QUERY}" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
${CLICKHOUSE_CLIENT} --user "${U2}" -q "${READ_QUERY}" 2>&1 | grep -o -m1 "QUOTA_EXCEEDED"
read_usage

cleanup
