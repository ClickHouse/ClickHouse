#!/usr/bin/env bash
# Grants on the tables that inserts into a `Merge` table are forwarded to.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_05897_${CLICKHOUSE_DATABASE}"
definer="definer_05897_${CLICKHOUSE_DATABASE}"
user_client="$CLICKHOUSE_CLIENT --user $user --password password"

# Prints the error code of a query, or OK.
function result()
{
    local out
    if out=$($user_client --send_logs_level=none "$@" 2>&1); then
        echo "OK"
    else
        echo "$out" | grep -oE '\(([A-Z_]+)\)' | head -n1
    fi
}

$CLICKHOUSE_CLIENT --multiline -q "
    DROP USER IF EXISTS $user, $definer;
    CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE USER $definer IDENTIFIED WITH plaintext_password BY 'password';

    CREATE TABLE target (a UInt32, b String DEFAULT 'default', secret String DEFAULT 'secret') ENGINE = MergeTree ORDER BY a;
    CREATE TABLE m (a UInt32) ENGINE = Merge(currentDatabase(), '^target$', target);
    CREATE TABLE m_ab (a UInt32, b String) ENGINE = Merge(currentDatabase(), '^target$', target);
    CREATE TABLE m_other (x UInt32) ENGINE = Merge(currentDatabase(), '^target$', target);
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.m TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.m_ab TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.m_other TO $user;
"

echo "--- no grant on the table to write"
result -q "INSERT INTO m SETTINGS async_insert = 0 VALUES (1)"
echo "--- the same for asynchronous inserts, before the data is queued"
result -q "INSERT INTO m SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (2)"
result -q "INSERT INTO m SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (3)"
echo "--- the grant is checked before the columns of the table to write are compared"
result -q "INSERT INTO m_other VALUES (4)"

echo "--- a grant on the written columns is enough"
$CLICKHOUSE_CLIENT -q "GRANT INSERT(a) ON ${CLICKHOUSE_DATABASE}.target TO $user"
result -q "INSERT INTO m SETTINGS async_insert = 0 VALUES (5)"
result -q "INSERT INTO m SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (6)"
echo "--- but not if more columns are written"
result -q "INSERT INTO m_ab VALUES (7, 'b')"
$CLICKHOUSE_CLIENT -q "GRANT INSERT(b) ON ${CLICKHOUSE_DATABASE}.target TO $user"
result -q "INSERT INTO m_ab VALUES (8, 'b')"

echo "--- a chain through an Alias table needs the grants on every table"
# An Alias table forwards all columns and checks the grant on the whole table.
$CLICKHOUSE_CLIENT --multiline -q "
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target TO $user;
    CREATE TABLE target_alias ENGINE = Alias(currentDatabase(), 'target');
    CREATE TABLE m_alias (a UInt32) ENGINE = Merge(currentDatabase(), '^target_alias$', target_alias);
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.m_alias TO $user;
"
result -q "INSERT INTO m_alias VALUES (9)"
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target_alias TO $user"
result -q "INSERT INTO m_alias VALUES (10)"

echo "--- a materialized view with SQL SECURITY DEFINER writes with the grants of the definer, not of the user"
$CLICKHOUSE_CLIENT --multiline -q "
    CREATE TABLE src_definer (a UInt32) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE target_3 (a UInt32) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE m_3 (a UInt32) ENGINE = Merge(currentDatabase(), '^target_3$', target_3);
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src_definer TO $definer;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.m_3 TO $definer;
    CREATE MATERIALIZED VIEW mv_definer TO m_3 DEFINER = $definer SQL SECURITY DEFINER AS SELECT a + 200 AS a FROM src_definer;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.src_definer TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target_3 TO $user;
"
result -q "INSERT INTO src_definer VALUES (13)"
$CLICKHOUSE_CLIENT --multiline -q "
    REVOKE INSERT ON ${CLICKHOUSE_DATABASE}.target_3 FROM $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target_3 TO $definer;
"
result -q "INSERT INTO src_definer VALUES (14)"

echo "--- data"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
$CLICKHOUSE_CLIENT -q "SELECT 'target', * FROM target ORDER BY a"
$CLICKHOUSE_CLIENT -q "SELECT 'target_3', * FROM target_3 ORDER BY a"

$CLICKHOUSE_CLIENT --multiline -q "
    DROP VIEW mv_definer SYNC;
    DROP USER $user, $definer;
"
