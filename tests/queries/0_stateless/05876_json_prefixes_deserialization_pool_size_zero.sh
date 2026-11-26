#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A prefixes deserialization pool with no threads reads JSON prefixes serially.
${CLICKHOUSE_LOCAL} --query "
CREATE TABLE t (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t VALUES (1, '{\"a\": 1}'), (2, '{\"a\": 2, \"b\": \"value\"}'), (3, '{\"arr\": [{\"x\": 1, \"y\": {\"z\": 2}}]}');
SELECT data FROM t ORDER BY id SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1;
SELECT data.arr FROM t ORDER BY id SETTINGS merge_tree_use_prefixes_deserialization_thread_pool = 1;
" -- --max_prefixes_deserialization_thread_pool_size=0
