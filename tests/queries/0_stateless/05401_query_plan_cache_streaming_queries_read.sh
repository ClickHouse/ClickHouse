#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-shared-merge-tree
# `ReadFromTableStep` does not serialize the `STREAM` modifier, so a streaming read must not be
# cached: a hit would silently become an ordinary snapshot read. The identical query runs twice
# (a miss, then a would-be hit) and must behave like normal execution both times.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS "$@" 2>&1 | grep -o -m1 -E '^[0-9a-z]+$'
}

$CLICKHOUSE_CLIENT --query "
    SYSTEM DROP QUERY PLAN CACHE;
    DROP TABLE IF EXISTS t_stream;
    CREATE TABLE t_stream (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    SYSTEM STOP MERGES t_stream;
    INSERT INTO t_stream SELECT number FROM numbers(5);
    INSERT INTO t_stream SELECT number FROM numbers(5, 5);
"
STREAM_QUERY="SELECT countIf(1) FROM t_stream STREAM BOUNDED CURSOR {'all': {'block_number': 1000000}}"
run --enable_streaming_queries 1 --query "$STREAM_QUERY"
run --enable_streaming_queries 1 --query "$STREAM_QUERY"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_stream"
