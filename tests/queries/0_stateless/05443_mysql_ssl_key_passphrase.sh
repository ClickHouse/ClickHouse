#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the MySQL integration is not available in the fast test build.
# no-replicated-database: a named collection is used.

# `ssl_key_passphrase` is the passphrase of an encrypted `ssl_key` / `ssl_key_pem`. It is a secret
# (masked like a password) and is accepted from SQL. It is useless without a client key.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

format() {
    echo "--- $1"
    local formatted
    formatted=$(echo "$2" | $CLICKHOUSE_FORMAT --oneline)
    echo "$formatted"
    if echo "$formatted" | grep -q "$SECRET"; then
        echo "FAIL: secret leaked in formatted query"
    fi
}

format "mysql table function" \
    "SELECT * FROM mysql('127.0.0.1:3306', 'db', 't', 'u', '${SECRET}', ssl_key_pem = '${SECRET}', ssl_key_passphrase = '${SECRET}')"
format "MySQL table engine" \
    "CREATE TABLE t (x Int32) ENGINE = MySQL('127.0.0.1:3306', 'db', 't', 'u', '${SECRET}', ssl_key_pem = '${SECRET}', ssl_key_passphrase = '${SECRET}')"
format "MySQL database engine" \
    "CREATE DATABASE d ENGINE = MySQL('127.0.0.1:3306', 'db', 'u', '${SECRET}', ssl_key_pem = '${SECRET}', ssl_key_passphrase = '${SECRET}')"
format "MySQL dictionary source" \
    "CREATE DICTIONARY d (id UInt32) PRIMARY KEY id SOURCE(MYSQL(HOST '127.0.0.1' PORT 3306 USER 'u' PASSWORD '${SECRET}' DB 'db' TABLE 't' SSL_KEY_PEM '${SECRET}' SSL_KEY_PASSPHRASE '${SECRET}')) LAYOUT(FLAT()) LIFETIME(0)"

# The same credentials on a named collection: the collection name and `table` stay visible.
format "mysql table function, named collection" \
    "SELECT * FROM mysql(creds, ssl_key_pem = '${SECRET}', table = 't', ssl_key_passphrase = '${SECRET}')"
format "MySQL table engine, named collection" \
    "CREATE TABLE t (x Int32) ENGINE = MySQL(creds, ssl_key_pem = '${SECRET}', table = 't', ssl_key_passphrase = '${SECRET}')"
format "CREATE NAMED COLLECTION" \
    "CREATE NAMED COLLECTION creds AS host = '127.0.0.1', ssl_key_pem = '${SECRET}', ssl_key_passphrase = '${SECRET}'"

expect_error() {
    local pattern="$1"
    shift
    if "$@" 2>&1 | grep -q "$pattern"; then
        echo "OK"
    else
        echo "FAIL: expected an error matching: $pattern"
    fi
}

echo "--- passphrase without a key is rejected"
expect_error "ssl_key_passphrase" $CLICKHOUSE_CLIENT --query \
    "SELECT * FROM mysql('127.0.0.1:1', 'd', 't', 'u', 'p', ssl_key_passphrase = 'x')"

echo "--- passphrase with a key is accepted as an argument"
if $CLICKHOUSE_CLIENT --query \
    "SELECT * FROM mysql('127.0.0.1:1', 'd', 't', 'u', 'p', ssl_key_pem = 'not a key', ssl_key_passphrase = 'x')" 2>&1 \
    | grep -q "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"; then
    echo "FAIL: the passphrase was not recognized as an argument"
else
    echo "OK"
fi

NC="mysql_05443_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mysql_05443_t"

echo "--- named collection: passphrase without a key is rejected"
$CLICKHOUSE_CLIENT --query "
    CREATE NAMED COLLECTION ${NC} AS
        host = '127.0.0.1', port = 1, user = 'u', password = 'p', database = 'd', table = 't',
        ssl_key_passphrase = 'x'"
expect_error "ssl_key_passphrase" $CLICKHOUSE_CLIENT --query "SELECT * FROM mysql(${NC})"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION ${NC}"

echo "--- named collection: passphrase with a key is accepted"
$CLICKHOUSE_CLIENT --query "
    CREATE NAMED COLLECTION ${NC} AS
        host = '127.0.0.1', port = 1, user = 'u', password = 'p', database = 'd', table = 't',
        ssl_key_pem = 'not a key', ssl_key_passphrase = '${SECRET}'"
if $CLICKHOUSE_CLIENT --query "SELECT * FROM mysql(${NC})" 2>&1 \
    | grep -q "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"; then
    echo "FAIL: the named collection credentials were not recognized"
else
    echo "OK"
fi
if $CLICKHOUSE_CLIENT --query "
    CREATE TABLE mysql_05443_t (x Int32) ENGINE = MySQL(${NC})" 2>&1 \
    | grep -q "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"; then
    echo "FAIL: ENGINE = MySQL(named collection) rejected the passphrase"
else
    echo "OK"
fi

echo "--- named collection: stored query hides the passphrase"
stored=$($CLICKHOUSE_CLIENT --query "SELECT create_query FROM system.named_collections WHERE name = '${NC}'")
if echo "$stored" | grep -q "$SECRET"; then
    echo "FAIL: secret leaked in stored named collection"
elif echo "$stored" | grep -q "ssl_key_passphrase"; then
    echo "OK"
else
    echo "FAIL: stored named collection does not mention ssl_key_passphrase"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mysql_05443_t"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
