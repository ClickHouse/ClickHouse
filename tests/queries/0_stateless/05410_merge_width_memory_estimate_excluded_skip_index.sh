#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertical merge computes a skip index over several columns on its horizontal stage, so the source columns
# of such an index are priced as merged at once. A skip index listed in `exclude_materialize_skip_indexes_on_merge`
# is not computed by the merge at all, so its columns stay on the vertical stage and the merge keeps its width.
#
# The table has 16 columns, a single key column and a `minmax` index over all 15 other columns. The estimate is
# sized from the server's actual memory limit so that a horizontal merge affords exactly two parts, while a
# vertical merge with one key column affords all eight. `vertical_merge_algorithm_min_columns_to_activate = 11`
# makes the merge horizontal once the index moves its columns to the horizontal stage. The other settings make
# every range eligible for the vertical algorithm, see `05218_merge_width_memory_estimate_vertical`.

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
    local exclude=$2

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
        c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64,
        INDEX idx cityHash64(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15) TYPE minmax)
    ENGINE = MergeTree ORDER BY k
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        materialize_skip_indexes_on_merge = 1,
        exclude_materialize_skip_indexes_on_merge = '$exclude',
        min_parts_to_merge_at_once = 2,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
        merge_selector_algorithm = 'Simple',
        merge_selector_base = 1000,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        min_rows_for_full_part_storage = 0,
        allow_vertical_merges_from_compact_to_wide_parts = 1,
        vertical_merge_algorithm_min_columns_to_activate = 11,
        enable_vertical_merge_algorithm = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(0, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(2, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(3, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(4, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(5, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(6, 1);
    INSERT INTO $table SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(7, 1);
    SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
    SYSTEM START MERGES $table;

    OPTIMIZE TABLE $table;
    "

    wait_for_merges "$table"

    $CLICKHOUSE_CLIENT --query "
    SELECT sum(k), sum(c15), count() FROM $table;

    SYSTEM FLUSH LOGS part_log;

    -- The prediction has to match what the merge did.
    SELECT DISTINCT merge_algorithm, length(merged_from) FROM system.part_log
    WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'
    ORDER BY ALL;

    DROP TABLE $table;
    "
}

# The merge computes the index, so too few columns are left to gather and every merge is horizontal and capped.
echo 'index computed'
run t_merge_width_index_computed ''

# The merge skips the index, so every range merges vertically and takes all eight parts at once.
echo 'index excluded'
run t_merge_width_index_excluded 'idx'
