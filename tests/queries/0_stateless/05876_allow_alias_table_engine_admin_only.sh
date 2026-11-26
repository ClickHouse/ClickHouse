#!/usr/bin/env bash
# Creating a table with the `Alias` engine requires `allow_alias_table_engine`, and while the setting is
# disabled it cannot be re-enabled at the session or query level, so only an administrator can grant
# it through a settings profile. The default user of the test environment has it enabled in the
# server configuration (`tests/config/users.d/allow_alias_table_engine.yaml`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05876_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
"

echo "-- the setting from the configuration allows creating an alias"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE a ENGINE = Alias('src');
    SELECT count() FROM a;
"

echo "-- a session that disabled the setting cannot create an alias and cannot re-enable the setting"
$CLICKHOUSE_CLIENT -q "
    SET allow_alias_table_engine = 0;
    CREATE TABLE a2 ENGINE = Alias('src');
" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
$CLICKHOUSE_CLIENT -q "
    SET allow_alias_table_engine = 0;
    SET allow_alias_table_engine = 1;
" 2>&1 | grep -om1 "QUERY_IS_PROHIBITED"

echo "-- an existing alias keeps working without the setting"
$CLICKHOUSE_CLIENT -q "
    SET allow_alias_table_engine = 0;
    SELECT count() FROM a;
    DETACH TABLE a;
    ATTACH TABLE a;
    SELECT sum(x) FROM a;
"

echo "-- a user without the setting cannot create an alias nor enable the setting"
# A created user inherits the default profile of the test environment, which enables the
# setting, so disable it for the user explicitly.
$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} IDENTIFIED WITH no_password SETTINGS allow_alias_table_engine = 0;
    GRANT CREATE TABLE, SELECT, SHOW ON ${CLICKHOUSE_DATABASE}.* TO ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.a3 ENGINE = Alias('src')" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
$CLICKHOUSE_CLIENT --user "${user}" -q "SET allow_alias_table_engine = 1" 2>&1 | grep -om1 "QUERY_IS_PROHIBITED"
$CLICKHOUSE_CLIENT --user "${user}" -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.a3 ENGINE = Alias('src') SETTINGS allow_alias_table_engine = 1" 2>&1 | grep -om1 "QUERY_IS_PROHIBITED"

echo "-- an administrator grants the setting to the user, who can then create an alias"
$CLICKHOUSE_CLIENT -q "ALTER USER ${user} SETTINGS allow_alias_table_engine = 1"
$CLICKHOUSE_CLIENT --user "${user}" -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.a3 ENGINE = Alias('src');
    SELECT count() FROM ${CLICKHOUSE_DATABASE}.a3;
"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
