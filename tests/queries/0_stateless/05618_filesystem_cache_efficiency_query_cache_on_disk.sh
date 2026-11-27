#!/usr/bin/env bash
# A hit of the query cache on disk reads its entry from filesystem cache segments, so the cache efficiency counts it as reuse.
# Separate `clickhouse-local` processes share one cache directory, as in `05019_query_cache_on_disk_local`: the second one serves the
# result from disk because its in-memory query cache is empty.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/query_cache_efficiency_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/query_cache_efficiency_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
CONFIG

query="SELECT number % 10 AS k, sum(number) FROM numbers(100000) GROUP BY k ORDER BY k SETTINGS use_query_cache = true, query_cache_on_disk_cache_name = 'query_results' FORMAT Null"
efficiency="SELECT count() > 0 AS has_segments, sum(active_bytes) > 0 AS has_active_bytes, sum(active_bytes + passive_bytes + idle_bytes) = sum(downloaded_size) AS classes_add_up_to_downloaded_size FROM system.filesystem_cache WHERE cache_name = 'query_results' FORMAT TSKV"
hits="SELECT value > 0 AS served_from_disk FROM system.events WHERE event = 'QueryCacheOnDiskHits' FORMAT TSKV"

echo "-- The first process writes the entry"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}"

echo "-- The second process finds the entry idle, serves it from disk, and its bytes become active"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${efficiency}; ${query}; ${hits}; ${efficiency}"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
