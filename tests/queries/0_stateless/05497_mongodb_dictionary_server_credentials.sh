#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the MongoDB dictionary source needs USE_MONGODB

# A MongoDB dictionary created with a DDL query may use an authentication mechanism that takes its credentials from the
# environment of the server process only if `s3_allow_server_credentials_in_user_queries` is enabled both in the session
# that defines it (checked at `CREATE`, at `ATTACH` with the full definition and at `RESTORE`) and in the settings of the
# server, which loads the dictionary. The server keeps the setting disabled here. No MongoDB server is contacted: the
# dictionaries are loaded lazily, and the refused ones fail before connecting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_05497_${CLICKHOUSE_DATABASE}"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
OIDC_URI="mongodb://127.0.0.1:1/db?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s"

cleanup()
{
    # Neither ignore nor fuzz these drops.
    $CLICKHOUSE_CLIENT -m -q "
        SET ast_fuzzer_any_query = 0, ignore_drop_queries_probability = 0;
        DROP DICTIONARY IF EXISTS d_oidc;
        DROP DICTIONARY IF EXISTS d_scram;
        DROP DICTIONARY IF EXISTS d_named;
        DROP DICTIONARY IF EXISTS d_restored;
        DROP NAMED COLLECTION IF EXISTS ${NC};
    "
}
trap cleanup EXIT
cleanup

echo 'A session where the setting is disabled cannot create a dictionary with such a mechanism'
$CLICKHOUSE_CLIENT -m -q "
    SET s3_allow_server_credentials_in_user_queries = 0;
    CREATE DICTIONARY d_oidc (_id String, v String) PRIMARY KEY _id
    SOURCE(MONGODB(URI '${OIDC_URI}' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT()); -- { serverError ACCESS_DENIED }
    CREATE DICTIONARY d_scram (_id String, v String) PRIMARY KEY _id
    SOURCE(MONGODB(URI 'mongodb://user:pass@127.0.0.1:1/db?authMechanism=SCRAM-SHA-256' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT());
    DROP DICTIONARY d_scram;
"

echo 'A session where the setting is enabled can create it, and the server loads it with its own settings'
$CLICKHOUSE_CLIENT -m -q "
    SET s3_allow_server_credentials_in_user_queries = 1;
    CREATE DICTIONARY d_oidc (_id String, v String) PRIMARY KEY _id
    SOURCE(MONGODB(URI '${OIDC_URI}' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT());
    SELECT dictGet('d_oidc', 'v', tuple('k')); -- { serverError ACCESS_DENIED }
"

echo 'Its stored definition is attached again in a session where the setting is disabled'
$CLICKHOUSE_CLIENT -m -q "
    SET s3_allow_server_credentials_in_user_queries = 0;
    DETACH DICTIONARY d_oidc;
    ATTACH DICTIONARY d_oidc;
    SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd_oidc';
"

echo 'The options that a dictionary adds to a named collection are checked'
$CLICKHOUSE_CLIENT -m -q "
    SET ast_fuzzer_any_query = 0;
    CREATE NAMED COLLECTION ${NC} AS host = '127.0.0.1', port = '1', db = 'db', user = '', password = '';
    SET s3_allow_server_credentials_in_user_queries = 0;
    CREATE DICTIONARY d_named (_id String, v String) PRIMARY KEY _id
    SOURCE(MONGODB(NAME ${NC} OPTIONS 'authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:k8s' COLLECTION 'c'))
    LAYOUT(COMPLEX_KEY_DIRECT()); -- { serverError ACCESS_DENIED }
"

echo 'A definition restored from a backup is checked like a new one'
$CLICKHOUSE_CLIENT -m -q "
    BACKUP DICTIONARY ${CLICKHOUSE_DATABASE}.d_oidc TO ${BACKUP} FORMAT Null;
    SET s3_allow_server_credentials_in_user_queries = 0;
    RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d_oidc AS ${CLICKHOUSE_DATABASE}.d_restored FROM ${BACKUP} FORMAT Null; -- { serverError ACCESS_DENIED }
    SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd_restored';
    SET s3_allow_server_credentials_in_user_queries = 1;
    RESTORE DICTIONARY ${CLICKHOUSE_DATABASE}.d_oidc AS ${CLICKHOUSE_DATABASE}.d_restored FROM ${BACKUP} FORMAT Null;
    SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'd_restored';
"
