#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SimpleMergeSelector` checks a candidate range against the memory-derived cap before it removes the small
# parts at the right of the range (`merge_selector_enable_heuristic_to_remove_small_parts_at_right`). Removing
# them can make a range that would merge vertically merge horizontally, so the trimmed range has to fit the cap
# of a horizontal merge again.
#
# The table has 16 columns and a single key column. The estimate is sized from the server's actual memory limit
# so that a horizontal merge affords two parts and a vertical one many more. Seven parts of 5000 rows and a tail
# of one row reach the row threshold of the vertical algorithm together (35001 rows), but the seven parts alone
# do not. Without the second check the eight-part range was admitted as a vertical merge, the tail was trimmed,
# and seven parts were merged horizontally. Background merges may take some of the parts before `OPTIMIZE`
# does, so the test checks that no merge was horizontal and wider than the cap.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_trimmed_range;

CREATE TABLE t_merge_width_trimmed_range (c0 UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64)
ENGINE = MergeTree ORDER BY c0
SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    merge_selector_enable_heuristic_to_remove_small_parts_at_right = 1,
    merge_selector_min_age_to_disable_right_tail_heuristic = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 35001,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

SYSTEM STOP MERGES t_merge_width_trimmed_range;
"

# Random values, so that the large parts do not compress and the tail stays far below 1% of the range.
for i in {0..6}
do
    $CLICKHOUSE_CLIENT --query "
    INSERT INTO t_merge_width_trimmed_range SELECT
        number, rand64(1), rand64(2), rand64(3), rand64(4), rand64(5), rand64(6), rand64(7),
        rand64(8), rand64(9), rand64(10), rand64(11), rand64(12), rand64(13), rand64(14), rand64(15)
    FROM numbers($(( i * 5000 )), 5000)"
done
$CLICKHOUSE_CLIENT --query "INSERT INTO t_merge_width_trimmed_range SELECT number, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 FROM numbers(35000, 1)"

$CLICKHOUSE_CLIENT --query "
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_trimmed_range' AND active;
SYSTEM START MERGES t_merge_width_trimmed_range;
OPTIMIZE TABLE t_merge_width_trimmed_range;
"

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_trimmed_range'")" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
SELECT sum(c0), count() FROM t_merge_width_trimmed_range;

SYSTEM FLUSH LOGS part_log;

SELECT count() > 0, countIf(merge_algorithm = 'Horizontal' AND length(merged_from) > 2) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_trimmed_range' AND event_type = 'MergeParts';

DROP TABLE t_merge_width_trimmed_range;
"
