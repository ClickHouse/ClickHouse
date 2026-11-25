#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Per-part `PrefetchingConcat` concatenates the streams of a part right at the read. A `Merge`
# table computes the `ALIAS` columns of a child table, and casts its columns to the types of the
# `Merge` table, in the child plan, above the child reader. That per-row work must keep running on
# every read stream, so the child reader must not use `PrefetchingConcat`, while a read that only
# needs header plumbing keeps it. `EXPLAIN PIPELINE` does not show the child pipeline of a `Merge`
# table, so the built pipeline is inspected through `system.processors_profile_log`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_merge_alias_data;
DROP TABLE IF EXISTS t_merge_alias;
DROP TABLE IF EXISTS t_merge_alias_plain;
DROP TABLE IF EXISTS t_merge_alias_cast;
CREATE TABLE t_merge_alias_data (key UInt64, payload String, extracted String ALIAS JSONExtractString(payload, 'x'))
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_merge_alias_data SELECT number, toJSONString(map('x', toString(number))) FROM numbers(90000);
OPTIMIZE TABLE t_merge_alias_data FINAL;
CREATE TABLE t_merge_alias (key UInt64, extracted String) ENGINE = Merge(currentDatabase(), '^t_merge_alias_data\$');
CREATE TABLE t_merge_alias_plain (key UInt64, payload String) ENGINE = Merge(currentDatabase(), '^t_merge_alias_data\$');
CREATE TABLE t_merge_alias_cast (key UInt64, payload LowCardinality(String)) ENGINE = Merge(currentDatabase(), '^t_merge_alias_data\$');
"

SETTINGS="enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, log_processors_profiles = 1,
          merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2"

QID_PLAIN="${CLICKHOUSE_DATABASE}_plain"
QID_ALIAS="${CLICKHOUSE_DATABASE}_alias"
QID_CAST="${CLICKHOUSE_DATABASE}_cast"

$CLICKHOUSE_CLIENT --query_id "$QID_PLAIN" --query "SELECT key, payload FROM t_merge_alias_plain ORDER BY key FORMAT Null SETTINGS $SETTINGS"
$CLICKHOUSE_CLIENT --query_id "$QID_ALIAS" --query "SELECT key, extracted FROM t_merge_alias ORDER BY key FORMAT Null SETTINGS $SETTINGS"
$CLICKHOUSE_CLIENT --query_id "$QID_CAST" --query "SELECT key, payload FROM t_merge_alias_cast ORDER BY key FORMAT Null SETTINGS $SETTINGS"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS processors_profile_log"

$CLICKHOUSE_CLIENT --query "
-- Only header plumbing above the child reader: PrefetchingConcat is used.
SELECT 'plain', countIf(name = 'PrefetchingConcat') > 0
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_PLAIN';
-- The alias expression is computed above the child reader: the read goes in order,
-- PrefetchingConcat is not used, and there are more read streams than the three parts.
SELECT 'alias', countIf(name LIKE '%algorithm: InOrder%') > 0, countIf(name = 'PrefetchingConcat') = 0, countIf(name LIKE 'MergeTreeSelect%') > 3
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_ALIAS';
-- The same for a cast to the type of the Merge table.
SELECT 'cast', countIf(name LIKE '%algorithm: InOrder%') > 0, countIf(name = 'PrefetchingConcat') = 0, countIf(name LIKE 'MergeTreeSelect%') > 3
    FROM system.processors_profile_log WHERE event_date >= today() - 1 AND query_id = '$QID_CAST';
SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)), count(), sum(extracted = toString(key)) FROM (
    SELECT key, extracted FROM t_merge_alias ORDER BY key
    SETTINGS max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2);
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_merge_alias; DROP TABLE t_merge_alias_plain; DROP TABLE t_merge_alias_cast; DROP TABLE t_merge_alias_data;"
