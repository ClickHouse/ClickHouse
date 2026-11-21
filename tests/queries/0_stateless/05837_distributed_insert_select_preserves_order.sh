#!/usr/bin/env bash
# Every `DistributedSink` sends its blocks to the shard in a separate insert, so with parallel sinks the table
# on the shard can get the blocks in another order. An `INSERT SELECT` with one stream into a `Distributed`
# table must keep a single sink. A `SELECT` with several streams has no input order to preserve, but the sink of
# a `Distributed` table forwards the write through separate inserts that hide the dependent views and the
# deduplication of the tables on the shards, so such an `INSERT SELECT` is kept single-stream as well, like a
# plain `INSERT` (see `insertDeduplicationNeedsSingleStream` and `serial_hidden_views` in `InterpreterInsertQuery`).

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INSERT_SETTINGS=(
    --max_threads=4 --max_insert_threads=4 --max_block_size=10000
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --use_strict_insert_block_limits=0 --deduplicate_insert=disable
    --async_insert=0 --distributed_foreground_insert=1 --parallel_distributed_insert_select=0
    --max_insert_block_size=10000 --min_insert_block_size_rows=10000 --min_insert_block_size_bytes=0
)

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE dist_order_local (k UInt32, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
    CREATE TABLE dist_order (k UInt32, v UInt64)
        ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_order_local);
"

# `numbers` has one output stream; `numbers_mt` has several.
for source in numbers numbers_mt; do
    echo "$source"
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
        "EXPLAIN PIPELINE INSERT INTO dist_order SELECT number % 10, number FROM $source(100000)" \
        | grep -c "DistributedSink" || true
done

# Ten blocks with the same ten keys: the table on the shard must keep the rows of the last block.
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "INSERT INTO dist_order SELECT number % 10, number FROM numbers(100000)"

$CLICKHOUSE_CLIENT --multiquery -q "
    OPTIMIZE TABLE dist_order_local FINAL;
    SELECT count(), countIf(v != 99990 + k) FROM dist_order_local;
    DROP TABLE dist_order;
    DROP TABLE dist_order_local;
"
