#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "-- An obsolete setting on the command line is refused"
$CLICKHOUSE_CLIENT --max_memory_usage_for_all_queries=123 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete setting ['max_memory_usage_for_all_queries'] is specified on the command line"
$CLICKHOUSE_LOCAL --max_memory_usage_for_all_queries=123 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete setting ['max_memory_usage_for_all_queries'] is specified on the command line"
$CLICKHOUSE_LOCAL --enable_analyzer=1 --multiple_joins_rewriter_version=2 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete settings ['allow_experimental_analyzer', 'multiple_joins_rewriter_version'] are specified on the command line"
$CLICKHOUSE_CLIENT --allow_merge_tree_settings --min_rows_for_compact_part=123 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete setting ['min_rows_for_compact_part'] is specified on the command line"

echo "-- An obsolete setting in a query is accepted"
$CLICKHOUSE_CLIENT -q "SELECT 1 SETTINGS max_memory_usage_for_all_queries = 123"
$CLICKHOUSE_CLIENT -q "SET multiple_joins_rewriter_version = 2; SELECT 2"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&odbc_max_field_size=1" -d "SELECT 3"
$CLICKHOUSE_LOCAL -q "SELECT 4 SETTINGS max_memory_usage_for_all_queries = 123"
# An obsolete `MergeTree` setting, by its `merge_tree_`-prefixed name, in a query and in a settings profile.
$CLICKHOUSE_CLIENT -q "SET merge_tree_min_rows_for_compact_part = 123; SELECT 5"
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS u_${CLICKHOUSE_DATABASE};
    DROP SETTINGS PROFILE IF EXISTS p_${CLICKHOUSE_DATABASE};
    CREATE USER u_${CLICKHOUSE_DATABASE};
    CREATE SETTINGS PROFILE p_${CLICKHOUSE_DATABASE} SETTINGS merge_tree_min_bytes_for_compact_part = 123 TO u_${CLICKHOUSE_DATABASE};"
$CLICKHOUSE_CLIENT --user u_${CLICKHOUSE_DATABASE} -q "SELECT 6"
$CLICKHOUSE_CLIENT -q "
    DROP SETTINGS PROFILE p_${CLICKHOUSE_DATABASE};
    DROP USER u_${CLICKHOUSE_DATABASE};"

echo "-- ... and it is shown in system.warnings"
$CLICKHOUSE_CLIENT -q "
    SELECT message LIKE '%''max_memory_usage_for_all_queries''%'
       AND message LIKE '%''multiple_joins_rewriter_version''%'
       AND message LIKE '%''odbc_max_field_size''%'
       AND message LIKE '%''merge_tree_min_rows_for_compact_part''%'
       AND message LIKE '%''merge_tree_min_bytes_for_compact_part''%'
    FROM system.warnings WHERE message LIKE 'Queries or settings profiles changed obsolete settings since the server start: %'"

echo "-- ... and logged"
# The log entry can be written after the response is sent, so retry until it appears.
while clickhouse_test_time_left; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
    res=$($CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.text_log WHERE level = 'Warning' AND message LIKE 'Obsolete setting \`max_memory_usage_for_all_queries\` is changed%'")
    [ "$res" = 1 ] && break
    sleep 0.5
done
echo "$res"
