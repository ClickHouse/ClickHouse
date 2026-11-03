#!/usr/bin/env bash
# Tags: use-rocksdb

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

INSERT_SETTINGS=(
    --max_threads=4 --max_insert_threads=4
    --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --use_strict_insert_block_limits=0 --deduplicate_insert=never
    --async_insert=0 --input_format_parallel_parsing=0
    --max_insert_block_size=10000 --min_insert_block_size_rows=10000 --min_insert_block_size_bytes=0
)

DATA_FILE="${CLICKHOUSE_TMP}/plain_insert_order_${CLICKHOUSE_DATABASE}.tsv"
trap 'rm -f "$DATA_FILE"' EXIT

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE order_rmt (k UInt32, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
    CREATE TABLE order_cmt (k UInt32, sign Int8, v UInt64) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
    CREATE TABLE order_kv (k UInt32, v UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
    CREATE TABLE order_coalescing (k UInt32, v Nullable(UInt64)) ENGINE = CoalescingMergeTree ORDER BY k;
    CREATE TABLE order_versioned (k UInt32, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;
    CREATE TABLE order_mt (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
"

# Check the plain `INSERT` pipeline deterministically, independently of sink scheduling.
for table in order_rmt order_coalescing order_kv; do
    echo "$table"
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
        "EXPLAIN PIPELINE INSERT INTO $table VALUES (0, 0)" | grep -cE "MergeTreeSink|EmbeddedRocksDB.*Sink"
done
echo order_cmt
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
    "EXPLAIN PIPELINE INSERT INTO order_cmt VALUES (0, 1, 0)" | grep -c "MergeTreeSink"

# Order-independent destinations retain parallel sinks.
for table in order_versioned order_mt; do
    echo "$table"
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
        "EXPLAIN PIPELINE INSERT INTO $table VALUES (0, 0)" | grep -c "MergeTreeSink"
done

# An order-sensitive materialized-view target also keeps the source single-stream.
$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE order_source (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW order_mv TO order_rmt AS SELECT * FROM order_source;
"
echo order_mv
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO order_source VALUES (0, 0)" | grep -c "MergeTreeSink"
$CLICKHOUSE_CLIENT --multiquery -q "DROP TABLE order_mv; DROP TABLE order_source"

$CLICKHOUSE_CLIENT -q "SELECT number % 10, number FROM numbers(100000) FORMAT TSV" > "$DATA_FILE"
$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM STOP MERGES order_rmt;
    SYSTEM STOP MERGES order_coalescing;
    SYSTEM STOP MERGES order_cmt;
"
for table in order_rmt order_kv order_coalescing order_versioned; do
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "INSERT INTO $table FORMAT TSV" < "$DATA_FILE"
done

# Block numbers must follow the input even before the parts merge.
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 1, groupArray(last_value) = arraySort(groupArray(last_value))
    FROM
    (
        SELECT _part, max(v) AS last_value FROM order_rmt
        GROUP BY _part ORDER BY toUInt64(splitByChar('_', _part)[2])
    )
"
$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM START MERGES order_rmt;
    SYSTEM START MERGES order_coalescing;
    OPTIMIZE TABLE order_rmt FINAL;
    OPTIMIZE TABLE order_coalescing FINAL;
    OPTIMIZE TABLE order_versioned FINAL;
    SELECT count(), countIf(v != 99990 + k) FROM order_rmt;
    SELECT count(), countIf(v != 99990 + k) FROM order_kv;
    SELECT count(), countIf(ifNull(v, 0) != 99990 + k) FROM order_coalescing;
    SELECT count(), countIf(v != 99990 + k) FROM order_versioned;
"

$CLICKHOUSE_CLIENT -q "
    SELECT k, sign, v
    FROM
    (
        SELECT number % 10 AS k, intDiv(toInt64(number), 10) AS i,
            arrayJoin(if(i = 0, [(1::Int8, i)], [(-1::Int8, i - 1), (1::Int8, i)])) AS t,
            t.1 AS sign, t.2 AS v
        FROM numbers(100000)
    )
    ORDER BY i, k, sign FORMAT TSV
" > "$DATA_FILE"
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "INSERT INTO order_cmt FORMAT TSV" < "$DATA_FILE"
$CLICKHOUSE_CLIENT --multiquery -q "
    SYSTEM START MERGES order_cmt;
    OPTIMIZE TABLE order_cmt FINAL;
    SELECT count(), countIf(sign != 1 OR v != 9999) FROM order_cmt;
    DROP TABLE order_rmt;
    DROP TABLE order_cmt;
    DROP TABLE order_kv;
    DROP TABLE order_coalescing;
    DROP TABLE order_versioned;
    DROP TABLE order_mt;
"
