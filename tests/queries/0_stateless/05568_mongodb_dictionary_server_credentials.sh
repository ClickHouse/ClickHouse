#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MongoDB dictionary source needs USE_MONGODB

# A MongoDB dictionary created with a DDL query may never make the server authenticate with credentials of the server
# process: the source is built when the server loads the dictionary, also after the named collection it refers to
# has changed. The dictionaries are loaded by `dictGet`.
# Nothing listens on 127.0.0.1:1, so a dictionary that is not refused fails to connect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HOST="127.0.0.1:1"
TIMEOUTS="connectTimeoutMS=100&serverSelectionTimeoutMS=100"
OIDC_URI="mongodb://${HOST}/db?${TIMEOUTS}&authMechanism=MONGODB-OIDC"
SCRAM_URI="mongodb://user:pass@${HOST}/db?${TIMEOUTS}&authMechanism=SCRAM-SHA-256"
NC="nc_05568_${CLICKHOUSE_DATABASE}"

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

# Runs the queries $1.
check()
{
    $CLICKHOUSE_CLIENT -m -q "$1" 2>&1 | classify
}

# Creates the dictionary $1 with the source $2 and prints whether reading it is refused.
check_source()
{
    $CLICKHOUSE_CLIENT -q "CREATE DICTIONARY $1 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB($2 COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT())"
    check "SELECT dictGet('$1', 'v', tuple('k'))"
}

cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        SET ast_fuzzer_any_query = 0, ignore_drop_queries_probability = 0;
        DROP DICTIONARY IF EXISTS d_uri;
        DROP DICTIONARY IF EXISTS d_options;
        DROP DICTIONARY IF EXISTS d_scram;
        DROP DICTIONARY IF EXISTS d_named_options;
        DROP DICTIONARY IF EXISTS d_named_altered;
        DROP NAMED COLLECTION IF EXISTS ${NC};
    "
}
trap cleanup EXIT
cleanup

echo 'Refused'
check_source d_uri "URI '${OIDC_URI}'"
check_source d_options "HOST '127.0.0.1' PORT 1 DB 'db' USER '' PASSWORD '' OPTIONS '${TIMEOUTS}&authMechanism=MONGODB-OIDC'"

echo 'Credentials in the connection string'
check_source d_scram "URI '${SCRAM_URI}'"

echo 'The options that a dictionary adds to a named collection'
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC} AS host = '127.0.0.1', port = '1', db = 'db', user = '', password = ''"
check_source d_named_options "NAME ${NC} OPTIONS '${TIMEOUTS}&authMechanism=MONGODB-OIDC'"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY d_named_options"

echo 'A named collection that changes after the dictionary is created'
$CLICKHOUSE_CLIENT -q "CREATE DICTIONARY d_named_altered (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(NAME ${NC} COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT())"
check "SELECT dictGet('d_named_altered', 'v', tuple('k'))"
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION ${NC} SET options = '${TIMEOUTS}&authMechanism=MONGODB-OIDC'"
check "SYSTEM RELOAD DICTIONARY d_named_altered; SELECT dictGet('d_named_altered', 'v', tuple('k'))"

echo 'clickhouse-local is not restricted'
$CLICKHOUSE_LOCAL -m -q "
    CREATE DICTIONARY d (_id String, v String) PRIMARY KEY _id
    SOURCE(MONGODB(URI '${OIDC_URI}' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT());
    SELECT dictGet('d', 'v', tuple('k'));
" 2>&1 | classify
