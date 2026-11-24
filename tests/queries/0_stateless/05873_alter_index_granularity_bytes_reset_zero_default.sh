#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A reset `index_granularity_bytes` falls back to the configured default. When that is zero, the reset would
# switch new parts to the non-adaptive mark format, so it is rejected, while a change that keeps the table
# adaptive is still allowed.
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
    ALTER TABLE t MODIFY SETTING index_granularity = 1024;
    ALTER TABLE t RESET SETTING index_granularity_bytes; -- { serverError READONLY_SETTING }
    SELECT extract(create_table_query, 'index_granularity = \\\\d+'), extract(create_table_query, 'index_granularity_bytes = \\\\d+')
    FROM system.tables WHERE database = currentDatabase() AND name = 't';
" -- --merge_tree.index_granularity_bytes=0
