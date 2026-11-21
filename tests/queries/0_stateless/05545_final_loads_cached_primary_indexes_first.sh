#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 5 intersecting parts, the primary index of each takes 300 KB, and the cache keeps 3 of them.
QUERIES="
CREATE TABLE t (k String, v UInt64, s UInt64, INDEX i s TYPE minmax GRANULARITY 1) ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS index_granularity = 1, use_primary_key_cache = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t SELECT leftPad(toString(number % 300 * 100 + intDiv(number, 300)), 5, '0') || repeat('a', 1000), number, number % 300 FROM numbers(1500)
SETTINGS max_block_size = 300, min_insert_block_size_rows = 300, min_insert_block_size_bytes = 0, max_insert_threads = 1;
"

# The indexes that are in the cache are not loaded again.
for condition in "1" "1" "s = 5" "k < '1'"
do
    QUERIES+="
    SELECT count(), sum(v) FROM t FINAL WHERE ${condition};
    SELECT 'loaded', value FROM system.events WHERE event = 'LoadedPrimaryIndexFiles';
    "
done

${CLICKHOUSE_LOCAL} --max_threads 1 --query "${QUERIES}" -- --primary_index_cache_size=1000000 --primary_index_cache_policy=LRU
