#!/usr/bin/env bash
# The nested `INSERT` of an `AliasSink` reaches tables that the outer insert does not see. These cases check
# how the outer insert counts its sinks when the `Alias` points to a materialized view, to a table made from a
# table function, and to a table with a long cascade of materialized views.

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

count_alias_sinks()
{
    $CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q "EXPLAIN PIPELINE $1" | grep -c "AliasSink" || true
}

# An `Alias` of a materialized view writes into the target of the view.
$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE behind_source (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE behind_mt (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE behind_rmt (k UInt32, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
    CREATE MATERIALIZED VIEW behind_view_mt TO behind_mt AS SELECT * FROM behind_source;
    CREATE MATERIALIZED VIEW behind_view_rmt TO behind_rmt AS SELECT * FROM behind_source;
    CREATE TABLE behind_alias_view_mt ENGINE = Alias('behind_view_mt');
    CREATE TABLE behind_alias_view_rmt ENGINE = Alias('behind_view_rmt');
"
echo "alias of a view to MergeTree"
count_alias_sinks "INSERT INTO behind_alias_view_mt VALUES (0, 0)"
echo "alias of a view to ReplacingMergeTree"
count_alias_sinks "INSERT INTO behind_alias_view_rmt VALUES (0, 0)"
for source in numbers numbers_mt; do
    count_alias_sinks "INSERT INTO behind_alias_view_rmt SELECT number % 10, number FROM $source(100000)"
done

# Ten blocks with the same ten keys: the target of the view must keep the rows of the last block.
$CLICKHOUSE_CLIENT "${INSERT_SETTINGS[@]}" -q \
    "INSERT INTO behind_alias_view_rmt SELECT number % 10, number FROM numbers(100000)"
$CLICKHOUSE_CLIENT --multiquery -q "
    OPTIMIZE TABLE behind_rmt FINAL;
    SELECT count(), countIf(v != 99990 + k) FROM behind_rmt;
"

# A table made from a table function is a proxy of the storage that the table function gives.
$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE behind_null AS null('k UInt32, v UInt64');
    CREATE TABLE behind_alias_null ENGINE = Alias('behind_null');
"
echo "alias of a table made from a table function"
count_alias_sinks "INSERT INTO behind_alias_null VALUES (0, 0)"

# A cascade of materialized views: cascade_0 -> cascade_1 -> ... -> cascade_16. The check of the tables behind
# an `Alias` stops at a fixed depth and then keeps one sink. An `Alias` of `cascade_0` has 16 views below it
# and reaches that depth; an `Alias` of `cascade_1` has 15 views below it and does not.
cascade="CREATE TABLE cascade_0 (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;"
for i in $(seq 1 16); do
    cascade+=" CREATE TABLE cascade_$i (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;"
    cascade+=" CREATE MATERIALIZED VIEW cascade_view_$i TO cascade_$i AS SELECT * FROM cascade_$((i - 1));"
done
cascade+=" CREATE TABLE cascade_alias_16 ENGINE = Alias('cascade_0');"
cascade+=" CREATE TABLE cascade_alias_15 ENGINE = Alias('cascade_1');"
$CLICKHOUSE_CLIENT --multiquery -q "$cascade"

echo "alias of a table with 15 cascaded views"
count_alias_sinks "INSERT INTO cascade_alias_15 VALUES (0, 0)"
echo "alias of a table with 16 cascaded views"
count_alias_sinks "INSERT INTO cascade_alias_16 VALUES (0, 0)"

cleanup="DROP TABLE cascade_alias_15; DROP TABLE cascade_alias_16;"
for i in $(seq 1 16); do
    cleanup+=" DROP TABLE cascade_view_$i;"
done
for i in $(seq 0 16); do
    cleanup+=" DROP TABLE cascade_$i;"
done
$CLICKHOUSE_CLIENT --multiquery -q "
    $cleanup
    DROP TABLE behind_alias_null;
    DROP TABLE behind_null;
    DROP TABLE behind_alias_view_mt;
    DROP TABLE behind_alias_view_rmt;
    DROP TABLE behind_view_mt;
    DROP TABLE behind_view_rmt;
    DROP TABLE behind_source;
    DROP TABLE behind_mt;
    DROP TABLE behind_rmt;
"
