#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MySQL client library is not built in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The MySQL client library reinterprets some host strings: a connection string (which can set a socket, a plugin
# directory, TLS files), a connection plugin URL, a list of hosts, or a host cut at a NUL. They must be rejected
# before any connection attempt.

# Prints how the host was rejected, or "connection failed" for an ordinary connection failure.
function check()
{
    local output
    output=$($CLICKHOUSE_CLIENT --send_logs_level=fatal -q "$1" 2>&1 | tr -d '\000')
    if grep -q -F "which the MySQL client library would read as" <<< "$output"; then
        grep -o -m1 "which the MySQL client library would read as [a-zA-Z ]*" <<< "$output"
    elif grep -q -F "ALL_CONNECTION_TRIES_FAILED" <<< "$output"; then
        echo "connection failed"
    else
        echo "unexpected output: $output"
    fi
}

check "SELECT * FROM mysql('socket=/nonexistent/mysqld.sock', 'db', 't', 'u', 'p')"
check "SELECT * FROM mysql('host=localhost;socket=/nonexistent/mysqld.sock', 'db', 't', 'u', 'p')"
check "SELECT * FROM mysql('127.0.0.1,localhost:1', 'db', 't', 'u', 'p')"
check "SELECT * FROM mysql('[replication://127.0.0.1]:1', 'db', 't', 'u', 'p')"
check "SELECT * FROM mysql('127.0.0.1\\0.example.com:1', 'db', 't', 'u', 'p')"

# A plain host is passed through and fails to connect as usual.
check "SELECT * FROM mysql('127.0.0.1:1', 'db', 't', 'u', 'p')"

# Port 0 would be replaced by the client library's default port.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT * FROM mysql('127.0.0.1:0', 'db', 't', 'u', 'p')" 2>&1 \
    | grep -q -F "MySQL port 0" && echo "port 0: rejected" || echo "port 0: connection failed"

# A dictionary created with a DDL query cannot set `socket` (see 05229_mysql_dictionary_socket), so port 0 is rejected
# for `localhost` too.
function check_dictionary()
{
    $CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
        CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.d_mysql (x UInt64, y String) PRIMARY KEY x
        SOURCE(MYSQL(HOST '$1' PORT 0 USER 'u' PASSWORD 'p' DB 'db' TABLE 't'))
        LAYOUT(FLAT()) LIFETIME(0)"
    local output
    output=$($CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.d_mysql" 2>&1)
    if grep -q -E "MySQL port 0|MySQL socket" <<< "$output"; then
        echo "dictionary $1 port 0: rejected"
    elif grep -q -F "through socket" <<< "$output"; then
        echo "dictionary $1 port 0: unix socket"
    else
        echo "unexpected output: $output"
    fi
    $CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.d_mysql"
}
check_dictionary "127.0.0.1"
check_dictionary "localhost"

# `localhost` uses TCP to the given port, not the client library's default Unix socket.
output=$($CLICKHOUSE_CLIENT --send_logs_level=fatal -q "SELECT * FROM mysql('localhost:1', 'db', 't', 'u', 'p')" 2>&1)
if grep -q -F "Can't connect to server on 'localhost'" <<< "$output"; then
    echo "localhost: tcp"
elif grep -q -F "through socket" <<< "$output"; then
    echo "localhost: unix socket"
else
    echo "unexpected output: $output"
fi

# The `MySQL` database engine and table engine use the same connection pool.
check "CREATE DATABASE ${CLICKHOUSE_DATABASE}_mysql ENGINE = MySQL('socket=/nonexistent/mysqld.sock', 'db', 'u', 'p')"
check "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_mysql (x UInt32) ENGINE = MySQL('socket=/nonexistent/mysqld.sock', 'db', 't', 'u', 'p'); SELECT * FROM ${CLICKHOUSE_DATABASE}.t_mysql"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_mysql; DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_mysql; DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.d_mysql"
