#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide, and SYSTEM DROP COLUMNS CACHE clears the
# cache of every other test.
# no-fasttest: a test that arms a fail point runs alone, and such tests are kept out of the fast test; depends on Parquet.
# no-random-settings: the part has to be read in many small tasks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM DROP COLUMNS CACHE` must be sticky against a reader of a `Parquet` part that was already
# running when it was issued. The reader creates its state anew for every task, and the state of
# a task that begins after the drop must not repopulate the cache.

FP=columns_cache_reader_pause_before_later_range

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# One part of 100 row groups, read in many tasks, so that a reader reads more than one task.
QUERY="SELECT sum(id), sum(length(value)) FROM t_parquet_cache_drop
    SETTINGS enable_parallel_replicas = 0, use_columns_cache = 1, max_threads = 2, max_block_size = 1000,
        merge_tree_min_read_task_size = 1, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_parquet_cache_drop;
    CREATE TABLE t_parquet_cache_drop (id UInt64, value String) ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 1000;
    INSERT INTO t_parquet_cache_drop SELECT number, toString(number) FROM numbers(100000);
"

# Control: without a drop in the middle, the read populates the cache.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "$QUERY"
$CLICKHOUSE_CLIENT --query "SELECT 'cached without a drop', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cache_drop'"

# The reader stops right before it creates the state of its second task, the cache is dropped, and the reader goes on.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --query "$QUERY" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $query_pid

$CLICKHOUSE_CLIENT --query "SELECT 'cached after a drop in the middle', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cache_drop'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_parquet_cache_drop"
