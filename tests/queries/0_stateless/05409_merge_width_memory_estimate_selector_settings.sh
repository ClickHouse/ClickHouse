#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# How `merge_memory_estimate_per_source_part_column` interacts with the key and the settings of the table:
# - with an empty sorting key, a merge forces the first column onto the horizontal stage before it adds the
#   columns of the skip indexes, so the estimate counts it too, and a range that has too few columns left to
#   gather merges horizontally and gets the width of a horizontal merge;
# - `min_parts_to_merge_at_once` above the memory-derived width is lowered to it, so the table still compacts;
# - without `allow_vertical_merges_from_compact_to_wide_parts` a range of wide parts still merges vertically,
#   so it keeps the width of a vertical merge.
#
# The tables have 16 columns. The estimate is sized from the server's actual memory limit so that a horizontal
# merge affords exactly two parts, while a vertical merge priced by a single key column affords all eight parts.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

# `merge_selector_base = 1000` is far above what eight fresh parts reach, so background merges do not take the
# parts, and `OPTIMIZE`, which selects with the base of one, merges them synchronously. The wait stays for a
# background merge that still gets through.
function wait_for_merges()
{
    local table=$1
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '$table'")" = 0 ] && return
        sleep 0.1
    done
    echo "Merges of $table did not finish"
}

function run()
{
    local table=$1
    local order_by=$2
    local index=$3
    local settings=$4

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
        c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64 $index)
    ENGINE = MergeTree ORDER BY $order_by
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
        merge_selector_algorithm = 'Simple',
        merge_selector_base = 1000,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        min_rows_for_full_part_storage = 0,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        $settings;
    "

    for i in {0..7}
    do
        $CLICKHOUSE_CLIENT --query "
        INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number,
            number, number, number, number, number, number
        FROM numbers($i, 1);
        "
    done

    $CLICKHOUSE_CLIENT --query "
    SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
    OPTIMIZE TABLE $table;
    "

    wait_for_merges "$table"

    $CLICKHOUSE_CLIENT --query "
    SELECT sum(c0), sum(c15), count() FROM $table;
    SYSTEM FLUSH LOGS part_log;
    SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
    WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'
    ORDER BY ALL;
    DROP TABLE $table;
    "
}

# The horizontal stage merges `c0`, forced by the empty key, and `c1` to `c5` of the index: ten columns are left to
# gather, fewer than eleven, so the merge runs horizontally and takes two parts.
echo 'empty key'
run t_merge_width_empty_key 'tuple()' ', INDEX idx cityHash64(c1, c2, c3, c4, c5) TYPE minmax' "
    min_parts_to_merge_at_once = 2,
    enable_vertical_merge_algorithm = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 11"

# A horizontal merge affords two parts, below the configured minimum of four.
echo 'minimum above the width'
run t_merge_width_min_parts 'c0' '' "
    min_parts_to_merge_at_once = 4,
    enable_vertical_merge_algorithm = 0,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1"

# All source parts are wide, so the merge runs vertically and takes all eight parts.
echo 'wide sources without compact to wide'
run t_merge_width_wide_sources 'c0' '' "
    min_parts_to_merge_at_once = 2,
    enable_vertical_merge_algorithm = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1"
