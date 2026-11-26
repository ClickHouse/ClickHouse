#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# `CHECK ALL TABLES` asks every table whether it is a `MergeTree` one, and an `Alias` answers that from its target,
# which loads a deferred table of a `lazy_load_tables` database. A user with `CHECK` on the alias but not on its
# target must not be able to load the target this way: loading constructs and starts the storage, and a failing
# constructor reports errors that depend on the definition of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CHECK ALL TABLES` goes through the databases from the last name to the first and stops at the first table
# it is denied, so the names make it visit the aliases before the tables they point to.
DB_ALIASES="zzzz_${CLICKHOUSE_DATABASE}_aliases"
DB="aaa_${CLICKHOUSE_DATABASE}_lazy"
USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine 1 -q "
    DROP DATABASE IF EXISTS \`$DB_ALIASES\` SYNC;
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    DROP USER IF EXISTS \`$USER\`;
    CREATE USER \`$USER\` IDENTIFIED WITH no_password;
    CREATE DATABASE \`$DB_ALIASES\` ENGINE = Atomic;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.ref (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE \`$DB\`.a_log (x UInt64) ENGINE = Log;
    CREATE TABLE \`$DB\`.broken (x UInt64, CONSTRAINT c CHECK x < (SELECT max(x) FROM \`$DB\`.ref)) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE \`$DB\`.valid (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`$DB\`.valid VALUES (1);
    GRANT SHOW DATABASES ON *.* TO \`$USER\`;
    GRANT CHECK ON \`$DB_ALIASES\`.* TO \`$USER\`;"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE \`$DB\`.ref SYNC"

function check_all_tables()
{
    echo "CHECK ALL TABLES: $($CLICKHOUSE_CLIENT --user "$USER" -q "CHECK ALL TABLES" < /dev/null 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|UNKNOWN_TABLE')"
    $CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"
}

# One alias at a time, so that each of them is the first table the user is denied.
for target in broken valid a_log
do
    echo "alias to $target"
    $CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine 1 -q "CREATE TABLE \`$DB_ALIASES\`.alias ENGINE = Alias(\`$DB\`, $target)"
    check_all_tables
    $CLICKHOUSE_CLIENT -q "DROP TABLE \`$DB_ALIASES\`.alias SYNC"
done

# With the right on the target, the table is loaded through the alias and checked.
echo "with the right on the target"
$CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine 1 -q "CREATE TABLE \`$DB_ALIASES\`.alias ENGINE = Alias(\`$DB\`, valid)"
$CLICKHOUSE_CLIENT -q "GRANT CHECK ON \`$DB\`.valid TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "CHECK TABLE \`$DB_ALIASES\`.alias SETTINGS check_query_single_value_result = 1"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

# `DROP DATABASE` loads the tables, so the one `broken` depends on is restored first.
$CLICKHOUSE_CLIENT -q "CREATE TABLE \`$DB\`.ref (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB_ALIASES\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
