#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A program of the `q` dialect can embed a query in SQL (`.ch.sql "..."`), and that query can hold credentials.
# They must not get to the query log: neither when the program is compiled and runs (the compiled query is logged
# without them), nor when the embedded query does not get to the compiled query or the compilation fails (the text
# of the program is logged, with the embedded query masked).

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

echo "--- the program runs, and the embedded query is the value of a variable that is not used"
$CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "t:.ch.sql \"SELECT dummy FROM remote('127.0.0.1', system.one, '${user}', '${password}')\"; 1+1"

echo "--- the embedded query is passed in a variable, and its table is not used"
$CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "s:\"SELECT dummy FROM remote('127.0.0.1', system.one, '${user}', '${password}')\"; t:.ch.sql s; 1+1"

echo "--- the embedded query is passed to another name of the function"
$CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q -q "f:.ch.sql; t:f \"SELECT dummy FROM remote('127.0.0.1', system.one, '${user}', '${password}')\"; 1+1"

echo "--- the compilation fails: the remote server does not answer"
q ".ch.sql \"SELECT dummy FROM remote('127.0.0.1:1', system.one, '${user}', '${password}')\"" 'ALL_CONNECTION_TRIES_FAILED|NETWORK_ERROR'

echo "--- the compilation fails: the embedded query does not parse"
q ".ch.sql \"SELECT dummy FROM FROM remote('127.0.0.1:1', system.one, '${user}', '${password}')\"" 'SYNTAX_ERROR'

echo "--- the compilation fails: the embedded query in a variable does not parse"
q "s:\"SELECT dummy FROM FROM remote('127.0.0.1:1', system.one, '${user}', '${password}')\"; .ch.sql s" 'SYNTAX_ERROR'

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# The programs above are the only queries of the kind `Select` of this test, apart from the query to the log itself.
echo "--- the queries are in the log, and the password is not"
$CLICKHOUSE_CLIENT -q "
    SELECT type, count() AS queries, countIf(position(query, '[HIDDEN]') > 0) AS hidden, countIf(position(query, '${password}') > 0) AS with_password
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND query_kind = 'Select' AND position(query, 'system.query_log') = 0
        AND type IN ('QueryFinish', 'ExceptionBeforeStart') AND is_initial_query
    GROUP BY type
    ORDER BY type
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
