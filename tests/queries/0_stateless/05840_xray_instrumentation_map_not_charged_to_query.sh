#!/usr/bin/env bash
# Tags: use-xray

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The XRay instrumentation map (about 170 MiB) is loaded by the first query that needs it and lives as long as the process,
# so that query must not be charged for it. A fresh process makes this query the first one.
${CLICKHOUSE_LOCAL} --allow_introspection_functions 1 --max_memory_usage 100M --query "
    SELECT count() > 0 FROM system.symbols WHERE symbol_demangled LIKE '%QueryMetricLog::startQuery%'
"
