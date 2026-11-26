#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

# The standalone `UPDATE` on a deferred table of a `lazy_load_tables` database: the rights are checked before
# the table is loaded, also through an `Alias`, and they are the ones a loaded table requires. The stand-in
# does not know that `_row_exists` is the lightweight-delete marker, so `UPDATE ... SET _row_exists = 0` must
# still require `ALTER DELETE` rather than `ALTER UPDATE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
USER_UPDATE="user_update_${CLICKHOUSE_DATABASE}"
USER_DELETE="user_delete_${CLICKHOUSE_DATABASE}"
USER_ALIAS="user_alias_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS \`$DB\` SYNC;
    DROP USER IF EXISTS \`$USER_UPDATE\`, \`$USER_DELETE\`, \`$USER_ALIAS\`;
    CREATE USER \`$USER_UPDATE\` IDENTIFIED WITH no_password;
    CREATE USER \`$USER_DELETE\` IDENTIFIED WITH no_password;
    CREATE USER \`$USER_ALIAS\` IDENTIFIED WITH no_password;
    CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`$DB\`.t (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    INSERT INTO \`$DB\`.t VALUES (1, 1), (2, 2);
    CREATE TABLE alias ENGINE = Alias(\`$DB\`, t) SETTINGS allow_experimental_alias_table_engine = 1;
    GRANT SELECT, ALTER UPDATE ON \`$DB\`.t TO \`$USER_UPDATE\`;
    GRANT SELECT, ALTER DELETE ON \`$DB\`.t TO \`$USER_DELETE\`;
    GRANT SELECT, ALTER ON ${CLICKHOUSE_DATABASE}.alias TO \`$USER_ALIAS\`;
"

function reattach()
{
    $CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\` SYNC"
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
}

function is_loaded()
{
    $CLICKHOUSE_CLIENT -q "SELECT name, is_loaded FROM system.tables WHERE database = '$DB' AND name = 't'"
}

function update()
{
    local user=$1
    local query=$2
    local result
    result=$($CLICKHOUSE_CLIENT --user "$user" --enable_lightweight_update 1 -q "$query" < /dev/null 2>&1)
    if grep -q -F 'ACCESS_DENIED' <<< "$result"; then
        echo "ACCESS_DENIED"
    elif grep -q -F 'Exception' <<< "$result"; then
        echo "$result"
    else
        echo "OK"
    fi
}

echo "through an alias without the rights on the target"
reattach
update "$USER_ALIAS" "UPDATE alias SET y = 2 WHERE x = 1"
update "$USER_ALIAS" "UPDATE alias SET _row_exists = 0 WHERE x = 1"
is_loaded

echo "_row_exists = 0 with ALTER UPDATE only"
update "$USER_UPDATE" "UPDATE \`$DB\`.t SET _row_exists = 0 WHERE 1"
is_loaded

echo "an ordinary update with ALTER DELETE only"
update "$USER_DELETE" "UPDATE \`$DB\`.t SET y = 10 WHERE 1"
is_loaded

echo "an ordinary update with ALTER UPDATE only"
update "$USER_UPDATE" "UPDATE \`$DB\`.t SET y = 10 WHERE x = 1"
is_loaded
$CLICKHOUSE_CLIENT -q "SELECT x, y FROM \`$DB\`.t ORDER BY x"

echo "_row_exists = 0 with ALTER DELETE only"
reattach
is_loaded
update "$USER_DELETE" "UPDATE \`$DB\`.t SET _row_exists = 0 WHERE x = 1"
is_loaded
$CLICKHOUSE_CLIENT -q "SELECT x, y FROM \`$DB\`.t ORDER BY x"

echo "the same on the loaded table"
update "$USER_UPDATE" "UPDATE \`$DB\`.t SET _row_exists = 0 WHERE 1"
update "$USER_ALIAS" "UPDATE alias SET y = 2 WHERE x = 2"
$CLICKHOUSE_CLIENT -q "SELECT x, y FROM \`$DB\`.t ORDER BY x"

$CLICKHOUSE_CLIENT -q "DROP TABLE alias SYNC"
$CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER \`$USER_UPDATE\`, \`$USER_DELETE\`, \`$USER_ALIAS\`"
