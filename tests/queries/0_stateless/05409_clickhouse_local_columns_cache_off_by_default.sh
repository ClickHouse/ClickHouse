#!/usr/bin/env bash

# `clickhouse-local` that processes its queries and exits has no columns cache by default: it would copy the
# columns it reads into the cache and rarely read them again. An explicit `columns_cache_size` enables it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT value FROM system.metrics WHERE metric = 'ColumnsCacheSizeLimit'"

${CLICKHOUSE_LOCAL} --query "${QUERY}"
${CLICKHOUSE_LOCAL} --query "${QUERY}" -- --columns_cache_size=100000000
