#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SymbolIndex` (about 25 MiB) is created by the first query that needs it and lives as long as the process,
# so that query must not be charged for it. A fresh process makes this query the first one.
${CLICKHOUSE_LOCAL} --allow_introspection_functions 1 --max_memory_usage 16M --query "
    SELECT count() FROM (SELECT addressToSymbol(number) FROM numbers(1))
"
