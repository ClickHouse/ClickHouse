#!/usr/bin/env bash
# Tags: zookeeper

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INSERT_SETTINGS=(
    --max_threads=4 --max_insert_threads=4 --max_block_size=1000
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --use_strict_insert_block_limits=0 --deduplicate_insert=disable
    --async_insert=0 --input_format_parallel_parsing=0 --parallel_view_processing=1
    --max_insert_block_size=1000 --min_insert_block_size_rows=1000 --min_insert_block_size_bytes=0
)
DATA_FILE="${CLICKHOUSE_TMP}/keeper_insert_order_${CLICKHOUSE_DATABASE}.tsv"
trap 'rm -f "$DATA_FILE"' EXIT

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE keeper_plain (k UInt32, v UInt64)
        ENGINE = KeeperMap('/' || currentDatabase() || '/keeper_plain') PRIMARY KEY k;
    CREATE TABLE keeper_select (k UInt32, v UInt64)
        ENGINE = KeeperMap('/' || currentDatabase() || '/keeper_select') PRIMARY KEY k;
    CREATE TABLE keeper_source (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW keeper_mv TO keeper_select AS SELECT * FROM keeper_source;
    CREATE TABLE keeper_alias ENGINE = Alias('keeper_source');
"
echo keeper_plain
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
    "EXPLAIN PIPELINE INSERT INTO keeper_plain VALUES (0, 0)" | grep -c "KeeperMapSink" || true
echo keeper_select
for source in numbers numbers_mt; do
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
        "EXPLAIN PIPELINE INSERT INTO keeper_select SELECT number % 10, number % 10 FROM $source(100000)" \
        | grep -c "KeeperMapSink" || true
done
echo keeper_alias
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "
    EXPLAIN PIPELINE INSERT INTO keeper_alias SELECT number % 10, number % 10 FROM numbers_mt(100000)
" | grep -c "AliasSink" || true

$CLICKHOUSE_CLIENT -q "SELECT number % 10, number FROM numbers(100000) FORMAT TSV" > "$DATA_FILE"
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "INSERT INTO keeper_plain FORMAT TSV" < "$DATA_FILE"
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --multiquery -q "
    SELECT count(), countIf(v != 99990 + k) FROM keeper_plain;
    INSERT INTO keeper_select SELECT number % 10, number % 10 FROM numbers_mt(100000);
    SELECT count(), countIf(v != k) FROM keeper_select;
    INSERT INTO keeper_alias SELECT number % 10, number % 10 FROM numbers_mt(100000);
    SELECT count(), countIf(v != k) FROM keeper_select;
    DROP TABLE keeper_alias;
    DROP TABLE keeper_mv;
    DROP TABLE keeper_source;
    DROP TABLE keeper_plain SYNC;
    DROP TABLE keeper_select SYNC;
"
