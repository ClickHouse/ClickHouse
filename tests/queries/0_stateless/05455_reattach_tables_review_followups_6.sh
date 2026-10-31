#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Queries that fail on an access check made only after the hook must not reattach any of their tables:
# - a read of a table with a stored expression calling `joinGet` on a `Join` table the caller cannot read,
#   joined with an otherwise eligible table;
# - the same with a stored expression calling `dictGet` on a dictionary the caller cannot use;
# - a read of a `Dictionary` table over a dictionary the caller cannot use.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_6_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP TABLE IF EXISTS t_join_get;
    DROP TABLE IF EXISTS t_dict_get;
    DROP TABLE IF EXISTS dict_table;
    DROP DICTIONARY IF EXISTS d;
    DROP TABLE IF EXISTS d_source;
    DROP TABLE IF EXISTS j;
    DROP TABLE IF EXISTS src;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE j (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO j VALUES (1, 10);
    CREATE TABLE d_source (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO d_source VALUES (1, 20);
    CREATE DICTIONARY d (k UInt64, v UInt64) PRIMARY KEY k
        SOURCE(CLICKHOUSE(TABLE 'd_source' DB '${CLICKHOUSE_DATABASE}')) LIFETIME(0) LAYOUT(FLAT());
    SYSTEM RELOAD DICTIONARY d;
    CREATE TABLE t_join_get (k UInt64, v UInt64 ALIAS joinGet('${CLICKHOUSE_DATABASE}.j', 'v', k)) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_join_get VALUES (1);
    CREATE TABLE t_dict_get (k UInt64, v UInt64 ALIAS dictGet('${CLICKHOUSE_DATABASE}.d', 'v', k)) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_dict_get VALUES (1);
    CREATE TABLE dict_table (k UInt64, v UInt64) ENGINE = Dictionary('${CLICKHOUSE_DATABASE}.d');
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_join_get TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_dict_get TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.dict_table TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

# Run the query as the restricted user; it must fail with the given error and must not detach any table.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$output" | grep -q "$2"; then
        echo "FAIL (unexpected error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}\."; then
        echo "FAIL (a table was detached: $output)"
    else
        echo "OK"
    fi
}

# A control: the same user can make the hook reattach the table.
function check_user_detaches()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "FAIL (client error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.$2"; then
        echo "OK"
    else
        echo "FAIL ($2 was not detached)"
    fi
}

# The hook skips a table whose exclusive lock it cannot take within a short probe, and on a loaded runner
# a background task of a fresh table can hold its lock for that long. Retry the positive control until the
# hook gets the table idle.
function retry_until_ok()
{
    local result
    for _ in {1..10}
    do
        result=$("$@")
        [ "$result" = "OK" ] && break
    done
    echo "$result"
}

echo "control"
retry_until_ok check_user_detaches "SELECT x FROM src" src

echo "stored joinGet in a join"
check_user_fails_without_detach "SELECT src.x, a.v FROM src CROSS JOIN t_join_get AS a" "ACCESS_DENIED"

echo "stored dictGet in a join"
check_user_fails_without_detach "SELECT src.x, a.v FROM src CROSS JOIN t_dict_get AS a" "ACCESS_DENIED"

echo "Dictionary table in a join"
check_user_fails_without_detach "SELECT src.x, dict_table.v FROM src CROSS JOIN dict_table" "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_join_get;
    DROP TABLE t_dict_get;
    DROP TABLE dict_table;
    DROP DICTIONARY d;
    DROP TABLE d_source;
    DROP TABLE j;
    DROP TABLE src;
    DROP USER ${USER_NAME};
"
