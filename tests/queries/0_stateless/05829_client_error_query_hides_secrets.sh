#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# In non-interactive mode a failed query is printed after its error as "(query: ...)"; it must hide secrets
# as the server's query log does. Statements carrying secrets in the client, in the client's own (local) checks, and
# in clickhouse-local.

user="no_such_user_${CLICKHOUSE_DATABASE}"
out_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
touch "$out_file"

# $1 = 1 also prints the echoed query; the rest is the command. Each statement runs once.
function check()
{
    local show_query=$1
    shift
    local output
    output=$("$@" 2>&1)
    echo "secret: $(grep -c 'SuperSecretKey123' <<< "$output"), echoed: $(grep -c '^(query: ' <<< "$output")"
    if [ "$show_query" = 1 ]; then
        grep '^(query: ' <<< "$output" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
    fi
    grep '^continued$' <<< "$output"
}

# A server error, a secret of the statement itself.
check 1 $CLICKHOUSE_CLIENT --query "ALTER USER $user IDENTIFIED WITH plaintext_password BY 'SuperSecretKey123'"

# A server error, a secret argument of a table function.
check 1 $CLICKHOUSE_CLIENT --query "SELECT * FROM remote('127.0.0.2', system.one, '$user', 'SuperSecretKey123')"

# A server error (no such database), a secret in the URL of a table engine.
check 1 $CLICKHOUSE_CLIENT --query "CREATE TABLE no_such_db_${CLICKHOUSE_DATABASE}.t (x UInt8) ENGINE = URL('http://user:SuperSecretKey123@localhost:11111/x', 'CSV')"

# A server error (the database exists), a secret argument of a database engine.
check 1 $CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE} ENGINE = Remote('127.0.0.2', 'system', '$user', 'SuperSecretKey123')"

# A server error (no such database), a secret of a dictionary source.
check 1 $CLICKHOUSE_CLIENT --query "CREATE DICTIONARY no_such_db_${CLICKHOUSE_DATABASE}.d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' PASSWORD 'SuperSecretKey123' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)"

# A server error (no such database), a secret in the inline data of an INSERT.
check 1 $CLICKHOUSE_CLIENT --query "INSERT INTO no_such_db_${CLICKHOUSE_DATABASE}.t VALUES (encrypt('aes-256-ecb', 'payload', 'SuperSecretKey123SuperSecretKey1'))"

# A client-side error (the output file exists), a secret argument of a backup destination.
check 0 $CLICKHOUSE_CLIENT --query "BACKUP TABLE system.one TO S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}', 'AKID', 'SuperSecretKey123') INTO OUTFILE '$out_file'"

# clickhouse-local reports the error and continues with --ignore-error.
check 0 $CLICKHOUSE_LOCAL --ignore-error --query "BACKUP TABLE system.one TO S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}', 'AKID', 'SuperSecretKey123') INTO OUTFILE '$out_file'; SELECT 'continued'"

# clickhouse-local, the inline data of an INSERT inside EXPLAIN.
check 1 $CLICKHOUSE_LOCAL --ignore-error --query "EXPLAIN INSERT INTO t VALUES (encrypt('aes-256-ecb', 'payload', 'SuperSecretKey123SuperSecretKey1')); SELECT 'continued'"

rm -f "$out_file"
