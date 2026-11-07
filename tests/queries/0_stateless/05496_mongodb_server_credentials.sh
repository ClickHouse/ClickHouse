#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MongoDB table function and table engine need USE_MONGODB

# A query that reads with the `mongodb` table function or from a `MongoDB` table is refused with `ACCESS_DENIED` if the
# connection string makes the server authenticate with credentials of the server process, while
# `s3_allow_server_credentials_in_user_queries` is disabled in the session that reads. Nothing listens on 127.0.0.1:1,
# so a query that is not refused fails to connect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HOST="127.0.0.1:1"
TIMEOUTS="connectTimeoutMS=100&serverSelectionTimeoutMS=100"
OIDC_URI="mongodb://${HOST}/db?${TIMEOUTS}&authMechanism=MONGODB-OIDC"

# Prints `refused` for `ACCESS_DENIED`, `connects` if the query got as far as connecting, or else the output.
classify()
{
    local output
    output=$(cat)
    if grep -q ACCESS_DENIED <<< "$output"; then
        echo refused
    elif grep -q 'No suitable servers found' <<< "$output"; then
        echo connects
    else
        echo "$output"
    fi
}

# Runs the query $2 with the setting $1.
check()
{
    $CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries "$1" -q "$2" 2>&1 | classify
}

read_uri()
{
    check "$1" "SELECT count() FROM mongodb('mongodb://${HOST}/db?${TIMEOUTS}&$2', 'c', 'x UInt8')"
}

cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        SET ast_fuzzer_any_query = 0, ignore_drop_queries_probability = 0;
        DROP TABLE IF EXISTS t_engine;
        DROP TABLE IF EXISTS t_as_function;
    "
}
trap cleanup EXIT
cleanup

echo 'Mechanisms that take the credentials from the server, and a client certificate file'
read_uri 0 'authMechanism=MONGODB-OIDC'
read_uri 0 'authmechanism=mongodb-oidc&authMechanismProperties=ENVIRONMENT:test'
read_uri 0 'authMechanism=MONGODB-AWS'
read_uri 0 'authMechanism=MONGODB-X509'
check 0 "SELECT count() FROM mongodb('mongodb://user@${HOST}/db?${TIMEOUTS}&authMechanism=GSSAPI', 'c', 'x UInt8')"
read_uri 0 'tls=true&tlsCertificateKeyFile=/nonexistent.pem'
read_uri 0 'ssl=true&sslClientCertificateKeyFile=/nonexistent.pem'
check 0 "SELECT count() FROM mongodb('${HOST}', 'db', 'c', '', '', 'x UInt8', '${TIMEOUTS}&authMechanism=MONGODB-OIDC')"

echo 'Credentials in the connection string'
check 0 "SELECT count() FROM mongodb('mongodb://user:pass@${HOST}/db?${TIMEOUTS}', 'c', 'x UInt8')"
check 0 "SELECT count() FROM mongodb('mongodb://user:pass@${HOST}/db?${TIMEOUTS}&authMechanism=SCRAM-SHA-256', 'c', 'x UInt8')"
check 0 "SELECT count() FROM mongodb('mongodb://user:pass@${HOST}/db?${TIMEOUTS}&authMechanism=plain', 'c', 'x UInt8')"

echo 'The setting permits the others'
read_uri 1 'authMechanism=MONGODB-OIDC'
read_uri 1 'tls=true&tlsCertificateKeyFile=/nonexistent.pem'

echo 'A MongoDB table is checked when it is read, with the setting of the session that reads'
$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries 0 -q "CREATE TABLE t_engine (x UInt8) ENGINE = MongoDB('${OIDC_URI}', 'c')"
check 0 "SELECT count() FROM t_engine"
check 1 "SELECT count() FROM t_engine"
$CLICKHOUSE_CLIENT -m -q "DETACH TABLE t_engine; ATTACH TABLE t_engine;"
check 0 "SELECT count() FROM t_engine"

echo 'So is a table created with CREATE TABLE ... AS mongodb(...)'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE t_as_function AS mongodb('${OIDC_URI}', 'c', 'x UInt8');
    DETACH TABLE t_as_function;
    ATTACH TABLE t_as_function;
    DESCRIBE t_as_function SETTINGS describe_compact_output = 1;
"
check 0 "SELECT count() FROM t_as_function"
check 1 "SELECT count() FROM t_as_function"
