#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge that removes expired values runs vertically only when no rows TTL reads a column that the merge expires,
# that is, a column absent from every source part and without a default (see `MergeTask::canVerticalTTLDelete`):
# only `TTLTransform`, after the merge, fills such a column. The memory estimate has to price such a merge as a
# horizontal one.
#
# The parts are inserted before the column `d` of the rows TTL is added, and the TTL is not materialized, so every
# merge of these parts removes expired values. The table then has 16 columns and a single key column. The estimate is
# sized from the server's actual memory limit so that a horizontal merge affords two parts, and a vertical one all
# eight. The control table has `d` in every part, so its merge runs vertically over all eight parts.

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
    local columns_before=$2
    local alter=$3

    $CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table;

    CREATE TABLE $table (k UInt64, $columns_before c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
        c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
        min_parts_to_merge_at_once = 2,
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
        merge_selector_algorithm = 'Simple',
        merge_selector_base = 1000,
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        min_rows_for_full_part_storage = 0,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        enable_vertical_merge_algorithm = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_bytes_to_activate = 0,
        vertical_merge_optimize_ttl_delete = 1,
        enable_block_number_column = 0,
        enable_block_offset_column = 0;

    SYSTEM STOP MERGES $table;
    INSERT INTO $table (k) SELECT number FROM numbers(0, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(1, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(2, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(3, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(4, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(5, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(6, 1);
    INSERT INTO $table (k) SELECT number FROM numbers(7, 1);
    $alter
    ALTER TABLE $table MODIFY TTL d + INTERVAL 100 YEAR SETTINGS materialize_ttl_after_modify = 0;
    SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
    SYSTEM START MERGES $table;

    OPTIMIZE TABLE $table;
    "

    wait_for_merges "$table"

    $CLICKHOUSE_CLIENT --query "
    SELECT sum(k), count() FROM $table;

    SYSTEM FLUSH LOGS part_log;

    SELECT merge_algorithm, max(length(merged_from)) <= 2, max(length(merged_from)) = 8 FROM system.part_log
    WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts'
    GROUP BY merge_algorithm
    ORDER BY ALL;

    DROP TABLE $table;
    "
}

run t_merge_width_ttl_absent_column "" "ALTER TABLE t_merge_width_ttl_absent_column ADD COLUMN d DateTime;"
run t_merge_width_ttl_present_column "d DateTime," ""
