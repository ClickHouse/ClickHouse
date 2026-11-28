#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# A user without the right to check or truncate a deferred table of a `lazy_load_tables` database must not be able
# to load it through `CHECK TABLE`, `CHECK ALL TABLES`, or `TRUNCATE TABLE` of an `Alias` that points to it: loading
# constructs and starts the storage. The error is `ACCESS_DENIED` both for a valid table and for one whose
# construction fails, which must not reveal its definition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CHECK ALL TABLES` goes through the databases from the last name to the first and stops at the first table
# it is denied, so the name makes this database the one it starts with.
DB="zzz_${CLICKHOUSE_DATABASE}_lazy"
USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --allow_experimental_alias_table_engine 1 -q "
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    DROP USER IF EXISTS \`$USER\`;
    CREATE USER \`$USER\` IDENTIFIED WITH no_password;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.ref (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE \`$DB\`.a_log (x UInt64) ENGINE = Log;
    CREATE TABLE \`$DB\`.broken (x UInt64, CONSTRAINT c CHECK x < (SELECT max(x) FROM \`$DB\`.ref)) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE \`$DB\`.valid (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`$DB\`.valid VALUES (1);
    CREATE TABLE alias_broken ENGINE = Alias(\`$DB\`, broken);
    CREATE TABLE alias_valid ENGINE = Alias(\`$DB\`, valid);
    GRANT SHOW DATABASES ON *.* TO \`$USER\`;
    GRANT CHECK, TRUNCATE ON ${CLICKHOUSE_DATABASE}.* TO \`$USER\`;"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "DROP TABLE \`$DB\`.ref SYNC"

for query in \
    "CHECK ALL TABLES" \
    "CHECK TABLE \`$DB\`.a_log" \
    "CHECK TABLE \`$DB\`.broken" \
    "CHECK TABLE \`$DB\`.valid" \
    "CHECK TABLE alias_broken" \
    "CHECK TABLE alias_valid" \
    "TRUNCATE TABLE alias_broken" \
    "TRUNCATE TABLE alias_valid" \
    "TRUNCATE TABLE IF EMPTY alias_broken" \
    "TRUNCATE TABLE IF EMPTY alias_valid"
do
    echo "${query//$DB/db}: $($CLICKHOUSE_CLIENT --user "$USER" -q "$query" < /dev/null 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|UNKNOWN_TABLE')"
done

$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

# With the rights on the target, the table is loaded and the commands go through.
$CLICKHOUSE_CLIENT -q "GRANT CHECK, TRUNCATE ON \`$DB\`.valid TO \`$USER\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "CHECK TABLE alias_valid SETTINGS check_query_single_value_result = 1"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT --user "$USER" -q "TRUNCATE TABLE alias_valid"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM \`$DB\`.valid"

# `DROP DATABASE` loads the tables, so the one `broken` depends on is restored first.
$CLICKHOUSE_CLIENT -q "CREATE TABLE \`$DB\`.ref (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_broken"
$CLICKHOUSE_CLIENT -q "DROP TABLE alias_valid"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
