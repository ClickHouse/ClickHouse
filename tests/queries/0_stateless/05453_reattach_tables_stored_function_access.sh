#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Tables the hook must leave alone although they are otherwise eligible:
# - a table with a stored column expression calling a function that checks privileges of its own (`file`),
#   which is expanded only during the analysis of the query, after the hook;
# - a `MergeTree` table defined with the legacy syntax, whose keys are stored in the engine arguments.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_stored_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP TABLE IF EXISTS t_alias;
    DROP TABLE IF EXISTS t_plain;
    DROP TABLE IF EXISTS legacy_t;
    CREATE TABLE t_alias (x UInt64, f String ALIAS file('anything', '')) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_alias (x) VALUES (1);
    CREATE TABLE t_plain (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_plain VALUES (1);
    SET allow_deprecated_syntax_for_merge_tree = 1;
    CREATE TABLE legacy_t (d Date, x UInt64) ENGINE = MergeTree(d, x, 8192);
    INSERT INTO legacy_t VALUES ('2026-01-01', 1);
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_alias TO ${USER_NAME};
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_plain TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

# Run the query as the restricted user and print whether it failed with the given error without detaching the table.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$output" | grep -q "$3"; then
        echo "FAIL (unexpected error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.$2"; then
        echo "FAIL ($2 was detached)"
    else
        echo "OK"
    fi
}

# A control: the same user can make the hook reattach a table without such expressions.
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
# a background task of a fresh table can hold its lock for that long. The positive controls are read-only,
# so retry them until the hook gets the table idle.
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

echo "stored function checking access"
retry_until_ok check_user_detaches "SELECT x FROM t_plain" t_plain
check_user_fails_without_detach "SELECT f FROM t_alias" t_alias "ACCESS_DENIED"
check_if_not_detached "SELECT x FROM t_alias" t_alias

echo "legacy MergeTree definition"
check_if_not_detached "SELECT count() FROM legacy_t" legacy_t

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM legacy_t; SELECT x FROM t_alias"

${CLICKHOUSE_CLIENT} -q "
    DROP USER ${USER_NAME};
    DROP TABLE t_alias;
    DROP TABLE t_plain;
    DROP TABLE legacy_t;
"
