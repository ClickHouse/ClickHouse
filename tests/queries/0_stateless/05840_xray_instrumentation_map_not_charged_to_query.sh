#!/usr/bin/env bash
# Tags: use-xray

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The XRay instrumentation map (about 170 MiB) is loaded by the first query that needs it and lives as long as the process,
# so that query must not be charged for it. A fresh process makes this query the first one.
# Small blocks: a full block of demangled names is about 32 MiB, and its buffer growth alone can exceed the limit.
${CLICKHOUSE_LOCAL} --allow_introspection_functions 1 --max_memory_usage 100M --max_block_size 8192 --query "
    SELECT count() > 0 FROM system.symbols WHERE symbol_demangled LIKE '%QueryMetricLog::startQuery%'
"
