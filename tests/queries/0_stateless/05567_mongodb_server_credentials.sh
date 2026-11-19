#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MongoDB table function and table engine need USE_MONGODB

# A query that reads with the `mongodb` table function or from a `MongoDB` table is refused with `ACCESS_DENIED` if the
# connection string makes the server authenticate with credentials of the server process. Nothing listens on
# 127.0.0.1:1, so a query that is not refused fails to connect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HOST="127.0.0.1:1"
TIMEOUTS="connectTimeoutMS=100&serverSelectionTimeoutMS=100"
OIDC_URI="mongodb://${HOST}/db?${TIMEOUTS}&authMechanism=MONGODB-OIDC"
NC="nc_05567_${CLICKHOUSE_DATABASE}"

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

check()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | classify
}

read_uri()
{
    check "SELECT count() FROM mongodb('mongodb://${HOST}/db?${TIMEOUTS}&$1', 'c', 'x UInt8')"
}

cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        SET ast_fuzzer_any_query = 0, ignore_drop_queries_probability = 0;
        DROP TABLE IF EXISTS t_engine;
        DROP TABLE IF EXISTS t_as_function;
        DROP NAMED COLLECTION IF EXISTS ${NC};
    "
}
trap cleanup EXIT
cleanup

echo 'Mechanisms that take the credentials from the server, and a client certificate file'
read_uri 'authMechanism=MONGODB-OIDC'
read_uri 'authmechanism=mongodb-oidc&authMechanismProperties=ENVIRONMENT:test'
read_uri 'authMechanism=MONGODB-AWS'
read_uri 'authMechanism=MONGODB-X509'
check "SELECT count() FROM mongodb('mongodb://user@${HOST}/db?${TIMEOUTS}&authMechanism=GSSAPI', 'c', 'x UInt8')"
read_uri 'tls=true&tlsCertificateKeyFile=/nonexistent.pem'
read_uri 'ssl=true&sslClientCertificateKeyFile=/nonexistent.pem'
check "SELECT count() FROM mongodb('${HOST}', 'db', 'c', '', '', 'x UInt8', '${TIMEOUTS}&authMechanism=MONGODB-OIDC')"

echo 'Credentials in the connection string'
check "SELECT count() FROM mongodb('mongodb://user:pass@${HOST}/db?${TIMEOUTS}', 'c', 'x UInt8')"
check "SELECT count() FROM mongodb('mongodb://user:pass@${HOST}/db?${TIMEOUTS}&authMechanism=SCRAM-SHA-256', 'c', 'x UInt8')"
check "SELECT count() FROM mongodb('mongodb://user:pass@${HOST}/db?${TIMEOUTS}&authMechanism=plain', 'c', 'x UInt8')"


echo 'A MongoDB table is checked when it is read'
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_engine (x UInt8) ENGINE = MongoDB('${OIDC_URI}', 'c')"
check "SELECT count() FROM t_engine"
$CLICKHOUSE_CLIENT -m -q "DETACH TABLE t_engine; ATTACH TABLE t_engine;"
check "SELECT count() FROM t_engine"

echo 'So is a table created with CREATE TABLE ... AS mongodb(...)'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE t_as_function AS mongodb('${OIDC_URI}', 'c', 'x UInt8');
    DETACH TABLE t_as_function;
    ATTACH TABLE t_as_function;
    DESCRIBE t_as_function SETTINGS describe_compact_output = 1;
"
check "SELECT count() FROM t_as_function"

echo 'And the table function with a named collection'
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC} AS uri = '${OIDC_URI}', collection = 'c', structure = 'x UInt8'"
check "SELECT count() FROM mongodb(${NC})"
