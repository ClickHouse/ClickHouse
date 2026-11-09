#!/usr/bin/env bash

# When `merge_memory_estimate_per_source_part_column` narrows merges down to two parts, background merges
# must still make progress for parts of unequal size. The ratio of the sum of two unequal parts to the larger
# of them is below two, so with the usual floor of two to which `merge_selector_base` is lowered, no merge
# would ever be accepted and the parts would accumulate.
#
# An absurdly large estimate makes even two columns exceed the budget, so the merge width falls to two.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_merge_width_bg;
    CREATE TABLE t_merge_width_bg (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS merge_memory_estimate_per_source_part_column = 1000000000000,
        merge_selector_algorithm = 'Simple',
        merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0;
    SYSTEM STOP MERGES t_merge_width_bg;
"

# Alternate parts of one row and of ten thousand rows, so that every two adjacent parts differ in size.
for i in $(seq 1 60); do
    if (( i % 2 )); then
        echo "INSERT INTO t_merge_width_bg SELECT number, $i FROM numbers(1);"
    else
        echo "INSERT INTO t_merge_width_bg SELECT number, $i FROM numbers(10000);"
    fi
done | $CLICKHOUSE_CLIENT -n

$CLICKHOUSE_CLIENT -q "SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_bg' AND active"

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_merge_width_bg"

# Wait for background merges to reduce the number of parts.
for _ in $(seq 1 600); do
    parts=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_bg' AND active")
    if (( parts < 50 )); then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SELECT 'compacted', count() < 50 FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_bg' AND active"

$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(v) FROM t_merge_width_bg;
    DROP TABLE t_merge_width_bg;
"
