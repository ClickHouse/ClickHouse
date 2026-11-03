#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The projection parts of a merge are merged by a child merge, which chooses the format of its result by the
# levels of the projection parts, not of the parent parts (see `MergeTask::MergeProjectionsStage::prepareProjections`).
# A merged projection part has level zero, so with `min_level_for_wide_part = 2` on the projection the child
# merge always produces a `Compact` part and runs horizontally, whatever the level of the parent parts. The
# estimate has to price the child merge as a horizontal one even when a source parent part is merged already.
#
# The projection has 16 columns. The estimate is sized from the server's actual memory limit so that a
# horizontal merge of 16 columns affords two parts, and a vertical merge priced by a single key column affords 32.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

INSERT_ROW="SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number"

# A background merge may still be running when `OPTIMIZE` finds nothing left to take, and its `part_log`
# entry appears only when it finishes.
function wait_for_merges()
{
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_projection_level'")" = 0 ] && return
        sleep 0.1
    done
    echo "Merges did not finish"
}

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_projection_level;

CREATE TABLE t_merge_width_projection_level (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64,
    PROJECTION p (SELECT * ORDER BY c1) WITH SETTINGS (min_level_for_wide_part = 2))
ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_level_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

-- A parent part of level 1, whose merged projection part has level 0.
SYSTEM STOP MERGES t_merge_width_projection_level;
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(0, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(1, 1);
SYSTEM START MERGES t_merge_width_projection_level;
OPTIMIZE TABLE t_merge_width_projection_level FINAL;
"

wait_for_merges

$CLICKHOUSE_CLIENT --query "
SELECT 'merged', level, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_projection_level' AND active;
SELECT 'merged projection', part_type FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_width_projection_level' AND active;

SYSTEM STOP MERGES t_merge_width_projection_level;
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(2, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(3, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(4, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(5, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(6, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(7, 1);
INSERT INTO t_merge_width_projection_level $INSERT_ROW FROM numbers(8, 1);
SELECT 'before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_projection_level' AND active;
SYSTEM START MERGES t_merge_width_projection_level;

OPTIMIZE TABLE t_merge_width_projection_level;
"

wait_for_merges

# The merges after the first one are capped by the horizontal child merge of the projection parts. Every
# projection part stays `Compact`.
$CLICKHOUSE_CLIENT --query "
SELECT sum(k), sum(c15), count() FROM t_merge_width_projection_level;
SELECT 'projection', groupUniqArray(part_type) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_width_projection_level' AND active;

SYSTEM FLUSH LOGS part_log;

SELECT 'widest merge', max(length(merged_from)) <= 2 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_projection_level' AND event_type = 'MergeParts'
    AND NOT has(merged_from, 'all_1_1_0');

DROP TABLE t_merge_width_projection_level;
"
