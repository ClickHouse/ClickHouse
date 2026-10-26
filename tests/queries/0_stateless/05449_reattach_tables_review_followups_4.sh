#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Cases the hook must handle although the tables are otherwise eligible:
# - a chain of `WITH` definitions the query never references;
# - a nested `SETTINGS profile` carrying settings the hook has to see (a table filter, the query cache);
# - a stored constraint with a scalar subquery reading the table itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_4_${CLICKHOUSE_DATABASE}"
FILTER_PROFILE="profile_reattach_filter_${CLICKHOUSE_DATABASE}"
CACHE_PROFILE="profile_reattach_cache_${CLICKHOUSE_DATABASE}"

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

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP SETTINGS PROFILE IF EXISTS ${FILTER_PROFILE};
    DROP SETTINGS PROFILE IF EXISTS ${CACHE_PROFILE};
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS secret;
    DROP TABLE IF EXISTS t_self_constraint;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE secret (y UInt64) ENGINE = MergeTree ORDER BY y;
    INSERT INTO secret VALUES (1);
    CREATE SETTINGS PROFILE ${FILTER_PROFILE} SETTINGS additional_table_filters = '{''src'': ''x IN (SELECT y FROM ${CLICKHOUSE_DATABASE}.secret)''}';
    CREATE SETTINGS PROFILE ${CACHE_PROFILE} SETTINGS use_query_cache = 1;
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

echo "chained unused WITH definitions"
check_if_not_detached "WITH a AS (SELECT x FROM src), unused AS (SELECT x FROM a) SELECT 1" src
# A control: a definition referenced through a used definition is collected.
retry_until_ok check_if_detached "WITH a AS (SELECT x FROM src), used AS (SELECT x FROM a) SELECT count() FROM used" src

# Run the query as the restricted user and report whether it succeeded and whether it detached `src`.
function check_user()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo -n "succeeded"
    elif echo "$output" | grep -q "ACCESS_DENIED"; then
        echo -n "ACCESS_DENIED"
    else
        echo -n "unexpected error: $output"
    fi
    if echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.src"; then
        echo ", detached"
    else
        echo ", not detached"
    fi
}

echo "nested settings profile"
# A control: the same user can make the hook reattach `src` through a subquery.
for _ in {1..10}
do
    result=$(check_user "SELECT x FROM (SELECT x FROM src)")
    [ "$result" = "succeeded, detached" ] && break
done
echo "$result"
check_user "SELECT x FROM (SELECT x FROM src SETTINGS profile = '${FILTER_PROFILE}')"
check_if_not_detached "SELECT count() FROM (SELECT x FROM src SETTINGS profile = '${CACHE_PROFILE}')" src

echo "constraint reading the table itself"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_self_constraint (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_self_constraint VALUES (1);
    ALTER TABLE t_self_constraint ADD CONSTRAINT c ASSUME x >= (SELECT count() FROM t_self_constraint);
"
check_if_not_detached "SELECT count() FROM t_self_constraint" t_self_constraint
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_self_constraint"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP SETTINGS PROFILE IF EXISTS ${FILTER_PROFILE};
    DROP SETTINGS PROFILE IF EXISTS ${CACHE_PROFILE};
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS secret;
    DROP TABLE IF EXISTS t_self_constraint;
"
