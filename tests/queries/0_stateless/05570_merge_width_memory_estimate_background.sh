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

for cap in 2 3 4; do
    table="t_merge_width_bg_$cap"
    # The cap is `memory_limit / 16 / columns / estimate`, and the table has two columns.
    estimate=$(( memory_limit / 32 / cap ))

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS merge_memory_estimate_per_source_part_column = $estimate,
            merge_selector_algorithm = 'Simple',
            merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0;
        SYSTEM STOP MERGES $table;
    "

    # Repeat parts of one row, ten thousand rows, one row and forty thousand rows, so that adjacent parts
    # differ in size and no few of them add up to twice the largest.
    for i in $(seq 1 60); do
        case $(( i % 4 )) in
            1|3) rows=1 ;;
            2) rows=10000 ;;
            0) rows=40000 ;;
        esac
        echo "INSERT INTO $table SELECT number, $i FROM numbers($rows);"
    done | $CLICKHOUSE_CLIENT -n

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
done
