#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# `SHOW COLUMNS` loads a table that a database with `lazy_load_tables` has not loaded yet, so that the key columns
# are reported. `SHOW_COLUMNS` can be granted for some columns only, and such a user sees those columns, so the
# table has to be loaded for them as well. A user without any right on the table must not load it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
user_column="user_column_${CLICKHOUSE_DATABASE}"
user_none="user_none_${CLICKHOUSE_DATABASE}"
CLIENT_IN_DB=${CLICKHOUSE_CLIENT/--database=$CLICKHOUSE_DATABASE/--database=$DB}

function reload()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\`"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
}

function loaded()
{
    $CLICKHOUSE_CLIENT -q "SELECT is_loaded FROM system.tables WHERE database = '$DB' AND name = 't'"
}

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user_column, $user_none;
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.t (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE \`$DB\`.other (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE USER $user_column, $user_none;
    GRANT SELECT(id) ON \`$DB\`.t TO $user_column;
    GRANT SELECT ON \`$DB\`.other TO $user_none;"

echo "- no right on the table"
reload
loaded
# The right on another table only lets the user connect to the database, and the user sees no columns of `t`.
$CLIENT_IN_DB --user "$user_none" -q "SHOW COLUMNS FROM t"
loaded

echo "- right on one column"
reload
loaded
$CLIENT_IN_DB --user "$user_column" -q "SHOW COLUMNS FROM t" | cut -f1,4
loaded

$CLICKHOUSE_CLIENT -q "
    DROP USER $user_column, $user_none;
    DROP DATABASE \`$DB\` SYNC;"
