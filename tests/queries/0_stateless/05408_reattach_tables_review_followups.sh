#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Queries the hook must leave alone although their tables are otherwise eligible:
# - a table whose stored projection cannot be analyzed under the settings the internal `ATTACH` runs with;
# - a nested `EXPLAIN`, which may not read the tables of the explained query;
# - `EXECUTE AS`, which checks `IMPERSONATE` and the access of another user;
# - a query run with `enable_global_with_statement = 0`, where a name of an outer CTE may be a table;
# - a query using a function that checks privileges of its own when it is built (`file`).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_${CLICKHOUSE_DATABASE}"
TARGET_USER_NAME="user_reattach_review_target_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP USER IF EXISTS ${TARGET_USER_NAME};
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS rhs;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE rhs (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO rhs VALUES (1);
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    CREATE USER ${TARGET_USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src TO ${TARGET_USER_NAME};
"

# Run the query as the restricted user; it must fail with the given error and must not detach `src`.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$output" | grep -q "$2"; then
        echo "FAIL (unexpected error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.src"; then
        echo "FAIL (src was detached)"
    else
        echo "OK"
    fi
}

# A control: the same user can make the hook reattach `src`.
function check_user_detaches()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "FAIL (client error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.src"; then
        echo "OK"
    else
        echo "FAIL (src was not detached)"
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

echo "setting-dependent projection"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_tz;
    DROP TABLE IF EXISTS t_plain;
    CREATE TABLE t_tz (dt DateTime, tz String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_tz VALUES ('2026-01-01 00:00:00', 'UTC');
    ALTER TABLE t_tz ADD PROJECTION p (SELECT dt, toTimezone(dt, tz) AS d ORDER BY dt) SETTINGS allow_nonconst_timezone_arguments = 1;
    CREATE TABLE t_plain (dt DateTime, tz String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_plain VALUES ('2026-01-01 00:00:00', 'UTC');
    ALTER TABLE t_plain ADD PROJECTION p (SELECT dt, tz ORDER BY tz);
"
# The internal `ATTACH` runs with the settings of the default profile. The test configuration
# (`tests/config/users.d/nonconst_timezone.xml`) enables `allow_nonconst_timezone_arguments` there, and then
# the projection is valid for the `ATTACH`, so the table may be reattached.
DEFAULT_ALLOWS_NONCONST_TIMEZONE=$(${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.settings_profile_elements
    WHERE profile_name = 'default' AND setting_name = 'allow_nonconst_timezone_arguments' AND value IN ('1', 'true')")
if [ "$DEFAULT_ALLOWS_NONCONST_TIMEZONE" -eq 0 ]; then
    check_if_not_detached "SELECT count() FROM t_tz SETTINGS optimize_use_projections = 0, allow_nonconst_timezone_arguments = 0" t_tz
else
    retry_until_ok check_if_detached "SELECT count() FROM t_tz SETTINGS optimize_use_projections = 0, allow_nonconst_timezone_arguments = 0" t_tz
fi
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_tz SETTINGS optimize_use_projections = 0"
# A control: a projection the default settings accept does not stop the reattach.
retry_until_ok check_if_detached "SELECT count() FROM t_plain" t_plain

echo "nested explain"
check_if_not_detached "SELECT * FROM (EXPLAIN AST SELECT * FROM src) FORMAT Null" src

echo "execute as"
retry_until_ok check_user_detaches "SELECT count() FROM src"
check_user_fails_without_detach "EXECUTE AS ${TARGET_USER_NAME} SELECT count() FROM src" "ACCESS_DENIED"

echo "global with statement"
check_user_fails_without_detach "WITH rhs AS (SELECT 1 AS x) SELECT x FROM src WHERE x IN (SELECT x FROM rhs) SETTINGS enable_global_with_statement = 0" "ACCESS_DENIED"
check_user_fails_without_detach "WITH rhs AS (SELECT 1 AS x) SELECT x FROM src WHERE x IN (SELECT x FROM rhs SETTINGS enable_global_with_statement = 0)" "ACCESS_DENIED"
# A control: with the outer CTE visible, `rhs` is the CTE and `src` is reattached.
retry_until_ok check_user_detaches "WITH rhs AS (SELECT 1 AS x) SELECT x FROM src WHERE x IN (SELECT x FROM rhs)"

echo "function privileges"
check_user_fails_without_detach "SELECT file('anything', '') FROM src" "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP USER IF EXISTS ${TARGET_USER_NAME};
    DROP TABLE IF EXISTS t_tz;
    DROP TABLE IF EXISTS t_plain;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS rhs;
"
