#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "-- An obsolete setting on the command line is refused"
$CLICKHOUSE_CLIENT --max_memory_usage_for_all_queries=123 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete setting ['max_memory_usage_for_all_queries'] is specified on the command line"
$CLICKHOUSE_LOCAL --max_memory_usage_for_all_queries=123 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete setting ['max_memory_usage_for_all_queries'] is specified on the command line"
$CLICKHOUSE_LOCAL --enable_analyzer=1 --multiple_joins_rewriter_version=2 -q "SELECT 1" 2>&1 | grep -o -F "Obsolete settings ['allow_experimental_analyzer', 'multiple_joins_rewriter_version'] are specified on the command line"

echo "-- An obsolete setting in a query is accepted"
$CLICKHOUSE_CLIENT -q "SELECT 1 SETTINGS max_memory_usage_for_all_queries = 123"
$CLICKHOUSE_CLIENT -q "SET multiple_joins_rewriter_version = 2; SELECT 2"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&odbc_max_field_size=1" -d "SELECT 3"
$CLICKHOUSE_LOCAL -q "SELECT 4 SETTINGS max_memory_usage_for_all_queries = 123"

echo "-- ... and it is shown in system.warnings"
$CLICKHOUSE_CLIENT -q "
    SELECT message LIKE '%''max_memory_usage_for_all_queries''%'
       AND message LIKE '%''multiple_joins_rewriter_version''%'
       AND message LIKE '%''odbc_max_field_size''%'
    FROM system.warnings WHERE message LIKE 'Queries or settings profiles changed obsolete settings since the server start: %'"

echo "-- ... and logged"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.text_log WHERE level = 'Warning' AND message LIKE 'Obsolete setting \`max_memory_usage_for_all_queries\` is changed%'"
