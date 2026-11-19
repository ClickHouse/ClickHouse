#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# A table with a `UNIQUE KEY` resolves a conflict between two rows by the order in which their blocks are
# committed, so an input with one stream must keep a single sink. A `SELECT` with several streams has no
# input order and keeps parallel sinks.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INSERT_SETTINGS=(
    --enable_unique_key=1
    --max_threads=4 --max_insert_threads=4 --max_block_size=1000
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --use_strict_insert_block_limits=0 --deduplicate_insert=disable
    --async_insert=0 --input_format_parallel_parsing=0
    --max_insert_block_size=1000 --min_insert_block_size_rows=1000 --min_insert_block_size_bytes=0
)

$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --multiquery -q "
    CREATE TABLE uk_ignore (k UInt32, v UInt64, payload String) ENGINE = MergeTree ORDER BY k UNIQUE KEY (k)
        SETTINGS unique_key_conflict_action = 'ignore';
    CREATE TABLE uk_overwrite (k UInt32, v UInt64, payload String) ENGINE = MergeTree ORDER BY k UNIQUE KEY (k)
        SETTINGS unique_key_conflict_action = 'overwrite';
    CREATE TABLE uk_no_key (k UInt32, v UInt64, payload String) ENGINE = MergeTree ORDER BY k;
"

# Sink counts: a plain `INSERT`, a `SELECT` with one stream (`numbers`), a `SELECT` with several streams (`numbers_mt`).
# The same table without the key keeps parallel sinks for every input.
for table in uk_ignore uk_overwrite uk_no_key; do
    echo "$table"
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
        "EXPLAIN PIPELINE INSERT INTO $table VALUES (0, 0, '')" | grep -c "MergeTreeSink" || true
    for source in numbers numbers_mt; do
        $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
            "EXPLAIN PIPELINE INSERT INTO $table SELECT number % 1000, number, '' FROM $source(100000)" \
            | grep -c "MergeTreeSink" || true
    done
done

# Ten blocks with the same 1000 keys. The first block is the largest, so with parallel sinks it is written
# last: `ignore` then keeps the rows of a later block instead of the first one.
for table in uk_ignore uk_overwrite; do
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "
        INSERT INTO $table SELECT number % 1000, number, if(number < 1000, repeat('x', 20000), '') FROM numbers(10000)
    "
done

$CLICKHOUSE_CLIENT --multiquery -q "
    SELECT count(), countIf(v != k) FROM uk_ignore;
    SELECT count(), countIf(v != 9000 + k) FROM uk_overwrite;
    DROP TABLE uk_ignore;
    DROP TABLE uk_overwrite;
    DROP TABLE uk_no_key;
"
