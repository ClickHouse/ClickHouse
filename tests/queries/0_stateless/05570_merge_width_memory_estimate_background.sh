#!/usr/bin/env bash

# When `merge_memory_estimate_per_source_part_column` narrows merges down to a few parts, background merges
# must still make progress for parts of unequal size. A merge is accepted when the sum of its parts is at
# least the lowered base times the largest of them, and for a few parts that grow geometrically that ratio
# stays below two, the usual floor to which `merge_selector_base` is lowered: no merge would ever be accepted
# and the parts would accumulate. The narrowed merges must also never exceed the cap.
#
# The estimate is derived from the server memory limit, so that the merge width is exactly the given cap.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

memory_limit=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")

# The base is lowered fully only when the partition has at least `max_parts_to_lower_base` (50) parts.
num_parts=56

run_cap()
{
    local cap=$1
    local table="t_merge_width_bg_$cap"
    # The cap is `memory_limit / 16 / columns / estimate`, and the table has two columns.
    local estimate=$(( memory_limit / 32 / cap ))

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS merge_memory_estimate_per_source_part_column = $estimate,
            merge_selector_algorithm = 'Simple',
            merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0;
        SYSTEM STOP MERGES $table;
    "

    # Repeat parts of one row, ten thousand rows, one row and forty thousand rows, so that adjacent parts
    # differ in size and no few of them add up to twice the largest. Create all of them with one query, as
    # separate inserts are slow in sanitizer builds: every block of forty thousand numbers is filtered down
    # to the size of its part, and squashing is disabled, so that each block becomes its own part.
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO $table
        SELECT number % 40000, intDiv(number, 40000) + 1 FROM numbers($num_parts * 40000)
        WHERE multiIf(intDiv(number, 40000) % 2 = 0, number % 40000 = 0, intDiv(number, 40000) % 4 = 1, number % 40000 < 10000, true)
        SETTINGS max_block_size = 40000, max_threads = 1, max_insert_threads = 1,
            min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
    "

    $CLICKHOUSE_CLIENT -q "SELECT $cap, 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active"

    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES $table"

    # Wait for background merges to reduce the number of parts.
    for _ in $(seq 1 600); do
        parts=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active")
        if (( parts < 50 )); then
            break
        fi
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "SELECT $cap, 'compacted', count() < 50 FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active"

    $CLICKHOUSE_CLIENT -q "
        SYSTEM STOP MERGES $table;
        SYSTEM FLUSH LOGS part_log;
        SELECT $cap, 'within cap', count() > 0, max(length(merged_from)) <= $cap
            FROM system.part_log WHERE database = currentDatabase() AND table = '$table' AND event_type = 'MergeParts';
        SELECT $cap, count(), sum(v) FROM $table;
        DROP TABLE $table;
    "
}

# The caps are independent, so check them concurrently: creating the parts dominates the runtime of the test.
for cap in 2 3 4; do
    run_cap "$cap" > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$cap.out" 2>&1 &
done
wait

for cap in 2 3 4; do
    cat "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$cap.out"
    rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$cap.out"
done
