#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The secrets in the inner query of `COPY (query) TO STDOUT` are hidden when the query is formatted
# and logged, as they are in the same query outside of `COPY`.
secret='CopySubquerySecret05495'
copy_query="COPY (SELECT * FROM remote('127.0.0.1', 'db', 't', 'u', '$secret')) TO STDOUT"

echo "-- formatted"
$CLICKHOUSE_FORMAT --oneline --query "$copy_query"
$CLICKHOUSE_FORMAT --oneline --query "$copy_query WITH (FORMAT csv, HEADER)"
$CLICKHOUSE_FORMAT --oneline --show_secrets --query "$copy_query"

# Only the PostgreSQL protocol serves `COPY`, so the native protocol rejects it after the query is logged.
echo "-- logged"
query_id="${CLICKHOUSE_DATABASE}_copy_subquery_hides_secrets"
$CLICKHOUSE_CLIENT --log_queries=1 --log_formatted_queries=1 --query_id "$query_id" --query "$copy_query" 2>&1 | grep -o -m1 UNKNOWN_TYPE_OF_QUERY
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"
$CLICKHOUSE_CLIENT --query "SELECT type, query, position(formatted_query, '[HIDDEN]') > 0, position(concat(query, formatted_query), '$secret') = 0 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT --query "SELECT countIf(message LIKE '%$secret%'), countIf(logger_name = 'executeQuery' AND message LIKE '%[HIDDEN]%') > 0 FROM system.text_log WHERE query_id = '$query_id' AND event_date >= yesterday() SETTINGS enable_parallel_replicas = 0"
