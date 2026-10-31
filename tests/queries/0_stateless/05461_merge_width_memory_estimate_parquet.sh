#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge whose result is a `Parquet` part always runs horizontally, so `merge_memory_estimate_per_source_part_column`
# has to price it by all its columns even when the vertical algorithm is enabled and the source parts are wide.
# The setup is the same as in `05218_merge_width_memory_estimate_vertical`: the estimate is sized so that a horizontal
# merge of this 16-column table affords exactly two parts, while a vertical merge would afford all four.

MEMORY_LIMIT=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_width_parquet;

CREATE TABLE t_merge_width_parquet (k UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
    c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64, c15 UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS merge_memory_estimate_per_source_part_column = $ESTIMATE,
    min_level_for_parquet_part = 1,
    min_parts_to_merge_at_once = 2,
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
    merge_selector_algorithm = 'Simple',
    merge_selector_base = 1000,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

SYSTEM STOP MERGES t_merge_width_parquet;
INSERT INTO t_merge_width_parquet SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(0, 1);
INSERT INTO t_merge_width_parquet SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(1, 1);
INSERT INTO t_merge_width_parquet SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(2, 1);
INSERT INTO t_merge_width_parquet SELECT number, number, number, number, number, number, number, number, number, number, number, number, number, number, number, number FROM numbers(3, 1);
SELECT 'before', count(), groupUniqArray(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_width_parquet' AND active;
SYSTEM START MERGES t_merge_width_parquet;

OPTIMIZE TABLE t_merge_width_parquet;
"

for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_merge_width_parquet'")" = 0 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "
SELECT sum(k), sum(c15), count() FROM t_merge_width_parquet;

SYSTEM FLUSH LOGS part_log;

-- Every merge produced a \`Parquet\` part horizontally, and so was capped at two parts.
SELECT DISTINCT merge_algorithm, length(merged_from), part_type FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_width_parquet' AND event_type = 'MergeParts'
ORDER BY ALL;

DROP TABLE t_merge_width_parquet;
"
