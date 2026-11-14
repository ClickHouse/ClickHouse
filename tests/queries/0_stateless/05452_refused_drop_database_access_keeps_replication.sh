#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_replicated"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
client="$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none"

$client -m -q "
    DROP USER IF EXISTS $user;
    CREATE DATABASE $db ENGINE = Replicated('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$db', 's1', 'r1');
    CREATE TABLE $db.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO $db.t SELECT number FROM numbers(1000);
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT DROP DATABASE ON $db.* TO $user;
"

# A refusal must not stop the database's replication: the DDL after it still works, and the data is kept.
check_replication()
{
    $client -m -q "
        CREATE TABLE $db.after_refusal (x UInt64) ENGINE = Memory;
        DROP TABLE $db.after_refusal SYNC;
        SELECT count() FROM $db.t;
    "
}

# The user may drop or detach the database, but not its table.
$client --user "$user" -q "DROP DATABASE $db" 2>&1 | grep -o -m1 "ACCESS_DENIED"
check_replication
$client --user "$user" -q "DETACH DATABASE $db" 2>&1 | grep -o -m1 "ACCESS_DENIED"
check_replication

# A dictionary in another database reads the table.
$client -q "
    CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.d (x UInt64) PRIMARY KEY x
    SOURCE(CLICKHOUSE(DB '$db' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)
"
$client -q "DROP DATABASE $db" 2>&1 | grep -o -m1 "HAVE_DEPENDENT_OBJECTS"
check_replication

$client -m -q "
    DROP DICTIONARY ${CLICKHOUSE_DATABASE}.d;
    DROP DATABASE $db SYNC;
    DROP USER $user;
"
