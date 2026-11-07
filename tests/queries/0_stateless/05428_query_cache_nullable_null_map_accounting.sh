#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Nullable` column without NULLs may have no null map, and a reading query may create it lazily
# in the shared column of an uncompressed query cache entry. The weight of the entry must not change on reads,
# otherwise the eviction subtracts more bytes than were added. `Arrow` over HTTP is formatted on the server.

# The tag is unique per test database, so the entry does not clash with other tests and the cache does not need to be cleared.
# The per-user quota of the query cache (`query_cache_max_size_in_bytes` in the test configuration) is shared with the tests
# running in parallel (and with the other copies of this test in the flaky check), so lift it, otherwise the entry may not be stored.
TAG="${CLICKHOUSE_DATABASE}_nullable"
SETTINGS="use_query_cache = 1, query_cache_compress_entries = 0, query_cache_squash_partial_results = 0, query_cache_max_size_in_bytes = 0, query_cache_max_entries = 0, query_cache_tag = '${TAG}'"

QUERY="SELECT toNullable(number), [toNullable(number)] FROM numbers(1000) SETTINGS ${SETTINGS}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "${QUERY} FORMAT Null"

SIZE_BEFORE=$(${CLICKHOUSE_CLIENT} --query "SELECT result_size FROM system.query_cache WHERE tag = '${TAG}'")
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "${QUERY} FORMAT Arrow" | wc -c | grep -qv '^0$' && echo "read"
SIZE_AFTER=$(${CLICKHOUSE_CLIENT} --query "SELECT result_size FROM system.query_cache WHERE tag = '${TAG}'")

[ -n "${SIZE_BEFORE}" ] && [ "${SIZE_BEFORE}" = "${SIZE_AFTER}" ] && echo "same size" || echo "size changed: ${SIZE_BEFORE} -> ${SIZE_AFTER}"
