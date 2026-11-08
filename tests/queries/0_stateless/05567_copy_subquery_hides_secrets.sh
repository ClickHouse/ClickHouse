#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The secrets in the inner query of `COPY (query) TO STDOUT` are hidden when the query is formatted
# and logged, as they are in the same query outside of `COPY`.
secret='CopySubquerySecret05567'
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

# `EXPLAIN AST` accepts `COPY`, so this one runs on the native protocol.
echo "-- explain"
explain_query_id="${CLICKHOUSE_DATABASE}_copy_subquery_explain"
$CLICKHOUSE_CLIENT --log_queries=1 --log_formatted_queries=1 --query_id "$explain_query_id" --query "EXPLAIN AST $copy_query" > /dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT type, position(query, '[HIDDEN]') > 0, position(concat(query, formatted_query), '$secret') = 0 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$explain_query_id' AND event_date >= yesterday() ORDER BY event_time_microseconds SETTINGS enable_parallel_replicas = 0"

# The PostgreSQL protocol still runs the inner query with its secrets. `prefer_localhost_replica = 0`
# makes `remote` connect and authenticate instead of reading the local table directly.
echo "-- postgresql"
PG_USER="pg_user_05567_${CLICKHOUSE_DATABASE}"
SECRET_USER="secret_user_05567_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER}, ${SECRET_USER};
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_05567 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.t_05567 VALUES (1), (2);
CREATE USER ${SECRET_USER} IDENTIFIED BY '${secret}';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05567 TO ${SECRET_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
GRANT REMOTE ON *.* TO ${PG_USER};
GRANT CREATE TEMPORARY TABLE ON *.* TO ${PG_USER};
"

psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --tuples-only --no-align 2>&1 <<SQL
COPY (SELECT x FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}', 't_05567', '${SECRET_USER}', '${secret}') ORDER BY x SETTINGS prefer_localhost_replica = 0) TO STDOUT;
SQL

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT count(), countIf(position(query, '$secret') = 0 AND position(query, '[HIDDEN]') > 0) FROM system.query_log WHERE event_date >= yesterday() AND user = '${PG_USER}' AND type = 'QueryStart' AND query LIKE '%remote(%' SETTINGS enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${CLICKHOUSE_DATABASE}.t_05567;
DROP USER ${PG_USER}, ${SECRET_USER};
"
