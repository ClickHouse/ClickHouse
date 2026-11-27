#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INSERT_SETTINGS=(
    --max_threads=4 --max_insert_threads=4 --max_block_size=10000
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --use_strict_insert_block_limits=0 --deduplicate_insert=disable
    --async_insert=0 --input_format_parallel_parsing=0 --parallel_view_processing=1
    --max_insert_block_size=10000 --min_insert_block_size_rows=10000 --min_insert_block_size_bytes=0
)
DATA_FILE="${CLICKHOUSE_TMP}/graphite_insert_order_${CLICKHOUSE_DATABASE}.tsv"
trap 'rm -f "$DATA_FILE"' EXIT

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE ordered_rmt (k UInt32, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
    CREATE TABLE ordered_versioned (k UInt32, v UInt64, ver UInt8 DEFAULT 1)
        ENGINE = ReplacingMergeTree(ver) ORDER BY k;
    CREATE TABLE ordered_cmt (k UInt32, v UInt64, sign Int8 DEFAULT 1)
        ENGINE = CollapsingMergeTree(sign) ORDER BY k;
    CREATE TABLE ordered_coalescing (k UInt32, v Nullable(UInt64)) ENGINE = CoalescingMergeTree ORDER BY k;
    CREATE TABLE ordered_mt (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW ordered_mv TO ordered_rmt AS SELECT * FROM ordered_mt;
    CREATE TABLE ordered_alias ENGINE = Alias('ordered_mt');
    CREATE TABLE ordered_outer (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW ordered_outer_mv TO ordered_alias AS SELECT * FROM ordered_outer;
    CREATE TABLE ordered_graphite (Path String, Time DateTime('UTC'), Value Float64, Version UInt64 DEFAULT 1)
        ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
"

# `numbers` has one output stream; `numbers_mt` has several and has no insertion order to retain.
for table in ordered_rmt ordered_versioned ordered_cmt ordered_coalescing ordered_mt ordered_alias ordered_outer; do
    echo "$table"
    for source in numbers numbers_mt; do
        $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
            "EXPLAIN PIPELINE INSERT INTO $table (k, v) SELECT number % 10, number FROM $source(100000)" \
            | grep -cE "MergeTreeSink|AliasSink" || true
    done
done
echo ordered_graphite
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
    "EXPLAIN PIPELINE INSERT INTO ordered_graphite (Path, Time, Value) VALUES ('0', 0, 1)" \
    | grep -c "MergeTreeSink" || true
for source in numbers numbers_mt; do
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "
        EXPLAIN PIPELINE INSERT INTO ordered_graphite (Path, Time, Value)
        SELECT toString(number % 10), toDateTime(0, 'UTC'), 100000 - number FROM $source(100000)
    " | grep -c "MergeTreeSink" || true
done

$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --multiquery -q "
    INSERT INTO ordered_rmt SELECT number % 10, number FROM numbers(100000);
    OPTIMIZE TABLE ordered_rmt FINAL;
    SELECT count(), countIf(v != 99990 + k) FROM ordered_rmt;
"
$CLICKHOUSE_CLIENT -q "
    SELECT toString(number % 10), toDateTime(0, 'UTC'), 100000 - number FROM numbers(100000) FORMAT TSV
" > "$DATA_FILE"
for input in plain select; do
    if [[ "$input" == plain ]]; then
        $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
            "INSERT INTO ordered_graphite (Path, Time, Value) FORMAT TSV" < "$DATA_FILE"
    else
        $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "
            INSERT INTO ordered_graphite (Path, Time, Value)
            SELECT toString(number % 10), toDateTime(0, 'UTC'), 100000 - number FROM numbers(100000)
        "
    fi
    $CLICKHOUSE_CLIENT --multiquery -q "
        OPTIMIZE TABLE ordered_graphite FINAL;
        SELECT count(), countIf(Value != 10 - toUInt64(Path)) FROM ordered_graphite;
        TRUNCATE TABLE ordered_graphite;
    "
done
$CLICKHOUSE_CLIENT --multiquery -q "
    DROP TABLE ordered_outer_mv;
    DROP TABLE ordered_outer;
    DROP TABLE ordered_alias;
    DROP TABLE ordered_mv;
    DROP TABLE ordered_mt;
    DROP TABLE ordered_rmt;
    DROP TABLE ordered_versioned;
    DROP TABLE ordered_cmt;
    DROP TABLE ordered_coalescing;
    DROP TABLE ordered_graphite;
"
