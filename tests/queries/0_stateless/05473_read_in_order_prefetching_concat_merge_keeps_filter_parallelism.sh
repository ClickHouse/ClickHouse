#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Per-part `PrefetchingConcat` concatenates the streams of a part right at the read. A `WHERE`
# over a `Merge` table is pushed down into the child plans, where it runs above the child reader.
# If it is not moved to `PREWHERE`, it must keep running on every read stream, so the child reader
# must not use `PrefetchingConcat`. `EXPLAIN PIPELINE` does not show the child pipeline of a `Merge`
# table, so the built pipeline is inspected through `system.processors_profile_log`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_filter_data;
DROP TABLE IF EXISTS t_merge_filter;
CREATE TABLE t_merge_filter_data (key UInt64, payload String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_merge_filter_data SELECT number, toJSONString(map('x', toString(number))) FROM numbers(90000);
OPTIMIZE TABLE t_merge_filter_data FINAL;
CREATE TABLE t_merge_filter AS t_merge_filter_data ENGINE = Merge(currentDatabase(), '^t_merge_filter_data\$');
"

SETTINGS="enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, log_processors_profiles = 1,
          merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2"

QID_PREWHERE="${CLICKHOUSE_DATABASE}_prewhere"
QID_FILTER="${CLICKHOUSE_DATABASE}_filter"

$CLICKHOUSE_CLIENT --query_id "$QID_PREWHERE" --query \
    "SELECT key FROM t_merge_filter WHERE JSONExtractString(payload, 'x') != '' ORDER BY key FORMAT Null
     SETTINGS $SETTINGS, optimize_move_to_prewhere = 1"

$CLICKHOUSE_CLIENT --query_id "$QID_FILTER" --query \
    "SELECT key FROM t_merge_filter WHERE JSONExtractString(payload, 'x') != '' ORDER BY key FORMAT Null
     SETTINGS $SETTINGS, optimize_move_to_prewhere = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS processors_profile_log"

$CLICKHOUSE_CLIENT --query "
-- The filter is evaluated in PREWHERE, inside every read stream: PrefetchingConcat is used.
SELECT 'prewhere', countIf(name = 'PrefetchingConcat') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_PREWHERE';
-- The filter stays above the child reader: the read goes in order, PrefetchingConcat is not used,
-- and there are more filters than the three parts.
SELECT 'filter_above_read', countIf(name LIKE '%algorithm: InOrder%') > 0, countIf(name = 'PrefetchingConcat') = 0, countIf(name = 'FilterTransform') > 3
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_FILTER';
SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)), count() FROM (
    SELECT key FROM t_merge_filter WHERE JSONExtractString(payload, 'x') != '' ORDER BY key
    SETTINGS max_threads = 6, optimize_read_in_order = 1, optimize_move_to_prewhere = 0,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2);
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_merge_filter; DROP TABLE t_merge_filter_data;"
