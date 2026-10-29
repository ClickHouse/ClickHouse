#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Queries that fail on an access check made only after the hook must not reattach any of their tables:
# - a read of a table with a stored expression calling a function that checks privileges of its own (`file`),
#   joined with an otherwise eligible table;
# - a read of a materialized view whose definer cannot read its target;
# - an insert into a `Buffer` table whose destination the caller cannot insert into;
# - an insert into a table with a materialized view whose definer cannot insert into its target.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_5_${CLICKHOUSE_DATABASE}"
DEFINER_NAME="definer_reattach_review_5_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP USER IF EXISTS ${DEFINER_NAME};
    DROP TABLE IF EXISTS mv;
    DROP TABLE IF EXISTS mv_ins;
    DROP TABLE IF EXISTS buf;
    DROP TABLE IF EXISTS t_alias;
    DROP TABLE IF EXISTS t_plain;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS dst;
    DROP TABLE IF EXISTS with_mv;
    DROP TABLE IF EXISTS mv_source;
    DROP TABLE IF EXISTS secret;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE with_mv (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE secret (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO secret VALUES (1);
    CREATE TABLE t_alias (x UInt64, f String ALIAS file('anything', '')) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_alias (x) VALUES (1);
    CREATE TABLE t_plain (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_plain VALUES (1);
    CREATE TABLE buf (x UInt64) ENGINE = Buffer(${CLICKHOUSE_DATABASE}, secret, 1, 1, 10, 1, 10, 1, 10);
    CREATE USER ${DEFINER_NAME} IDENTIFIED WITH no_password;
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    CREATE TABLE mv_source (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv TO secret DEFINER = ${DEFINER_NAME} SQL SECURITY DEFINER AS SELECT x FROM mv_source;
    CREATE MATERIALIZED VIEW mv_ins TO secret DEFINER = ${DEFINER_NAME} SQL SECURITY DEFINER AS SELECT x FROM with_mv;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_alias TO ${USER_NAME};
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_plain TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.mv TO ${USER_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.buf TO ${USER_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dst TO ${USER_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.with_mv TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

# Run the query as the restricted user; it must fail with the given error and must not detach any table.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --async_insert=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
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
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --async_insert=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
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
# a background task of a fresh table can hold its lock for that long. Retry the positive controls until the
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

echo "stored function checking access in a join"
retry_until_ok check_user_detaches "SELECT x FROM t_plain" t_plain
check_user_fails_without_detach "SELECT p.x, a.f FROM t_plain AS p CROSS JOIN t_alias AS a" "ACCESS_DENIED"

echo "materialized view read"
retry_until_ok check_user_detaches "SELECT x FROM src" src
check_user_fails_without_detach "SELECT src.x, mv.x FROM src CROSS JOIN mv" "ACCESS_DENIED"

echo "insert into Buffer"
retry_until_ok check_user_detaches "INSERT INTO dst SELECT x FROM src" src
check_user_fails_without_detach "INSERT INTO buf SELECT x FROM src" "ACCESS_DENIED"

echo "insert into a table with a materialized view"
check_user_fails_without_detach "INSERT INTO with_mv SELECT x FROM src" "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM secret"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE mv;
    DROP TABLE mv_ins;
    DROP TABLE buf;
    DROP TABLE t_alias;
    DROP TABLE t_plain;
    DROP TABLE src;
    DROP TABLE dst;
    DROP TABLE with_mv;
    DROP TABLE mv_source;
    DROP TABLE secret;
    DROP USER ${USER_NAME};
    DROP USER ${DEFINER_NAME};
"
