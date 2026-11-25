#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A program of the `q` dialect can embed a query in SQL (`.ch.sql "..."`), and that query can hold credentials.
# They must not get to the query log: neither when the program is compiled and runs (the compiled query is logged
# without them), nor when its compilation fails (the text of the program is logged, with the embedded query masked).

user="u_05880_${CLICKHOUSE_DATABASE}"
password="q_secret_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${password}';
    GRANT SELECT ON system.one TO ${user};
"

# Runs a program that fails, and prints whether the exception is of the expected kind.
function q()
{
    $CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "$1" 2>&1 | grep -q -E "$2" && echo "failed as expected"
}

echo "--- the program runs"
$CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q ".ch.sql \"SELECT dummy FROM remote('127.0.0.1', system.one, '${user}', '${password}')\""

echo "--- the compilation fails: the remote server does not answer"
q ".ch.sql \"SELECT dummy FROM remote('127.0.0.1:1', system.one, '${user}', '${password}')\"" 'ALL_CONNECTION_TRIES_FAILED|NETWORK_ERROR'

echo "--- the compilation fails: the embedded query does not parse"
q ".ch.sql \"SELECT dummy FROM FROM remote('127.0.0.1:1', system.one, '${user}', '${password}')\"" 'SYNTAX_ERROR'

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

echo "--- the queries are in the log, and the password is not"
$CLICKHOUSE_CLIENT -q "
    SELECT type, countIf(position(query, '[HIDDEN]') > 0) > 0 AS hidden, countIf(position(query, '${password}') > 0) AS with_password
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND position(query, 'system.one') > 0 AND position(query, 'system.query_log') = 0
        AND type IN ('QueryFinish', 'ExceptionBeforeStart')
    GROUP BY type
    ORDER BY type
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
