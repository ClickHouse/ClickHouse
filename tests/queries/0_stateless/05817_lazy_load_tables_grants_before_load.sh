#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# The check that keeps a user without rights from loading a deferred table of a `lazy_load_tables` database must not
# deny what the same user can do on a loaded table: read a virtual column granted alone, analyze a query with only
# `SHOW COLUMNS`, and delete rows with `UPDATE _row_exists = 0` with only `ALTER DELETE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    DROP USER IF EXISTS \`$USER\`;
    CREATE USER \`$USER\` IDENTIFIED WITH no_password;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.virtual (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`$DB\`.virtual VALUES (1);
    CREATE TABLE \`$DB\`.show_columns (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE \`$DB\`.alter_delete (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`$DB\`.alter_delete VALUES (1), (2);
    CREATE TABLE \`$DB\`.other (id UInt64) ENGINE = MergeTree ORDER BY id;
    GRANT SELECT(_part) ON \`$DB\`.virtual TO \`$USER\`;
    GRANT SHOW COLUMNS ON \`$DB\`.show_columns TO \`$USER\`;
    GRANT ALTER DELETE ON \`$DB\`.alter_delete TO \`$USER\`;"

$CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"

$CLICKHOUSE_CLIENT --user "$USER" -q "SELECT _part FROM \`$DB\`.virtual"
$CLICKHOUSE_CLIENT --user "$USER" -q "EXPLAIN QUERY TREE SELECT id FROM \`$DB\`.show_columns" | grep -c 'show_columns'
$CLICKHOUSE_CLIENT --user "$USER" -q "ALTER TABLE \`$DB\`.alter_delete UPDATE _row_exists = 0 WHERE id = 1 SETTINGS mutations_sync = 2"

# Without any grant the table stays unloaded.
echo "other: $($CLICKHOUSE_CLIENT --user "$USER" -q "SELECT _part FROM \`$DB\`.other" < /dev/null 2>&1 | grep -o -m1 'ACCESS_DENIED')"
echo "other: $($CLICKHOUSE_CLIENT --user "$USER" -q "ALTER TABLE \`$DB\`.other UPDATE _row_exists = 0 WHERE id = 1" < /dev/null 2>&1 | grep -o -m1 'ACCESS_DENIED')"
$CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT id FROM \`$DB\`.alter_delete"

$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER\`"
