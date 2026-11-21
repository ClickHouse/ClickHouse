#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# `reattach_tables_before_query_execution` must skip:
# - a table whose stored definition has an expression the querying user cannot rebuild on `ATTACH`
#   (a skip index over `demangle` without the `INTROSPECTION` grant);
# - a query calling an AI function, which checks `NAMED_COLLECTION` on its credentials when it runs;
# - a query reading sources supplied through `additional_table_filters` / `additional_result_filter`;
# - a query whose nested `WITH` scope is reset with `SETTINGS enable_global_with_statement = DEFAULT`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

TEST_USER="user_reattach_sess_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_sess_idx"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_sess_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_sess_secret"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --allow_introspection_functions=1 -q "CREATE TABLE t_reattach_sess_idx (s String, INDEX i demangle(s) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_sess_idx VALUES ('a')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_sess_src (a UInt64, s String) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_sess_src VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_sess_secret (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_sess_secret VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${TEST_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_sess_idx TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_sess_src TO ${TEST_USER}"

# Run a query as the test user with the hook enabled and report whether `table` was detached.
# `mode` is `ok` (the query must succeed) or the error code the query must fail with.
function run_as_user()
{
    local mode="$1" table="$2" query="$3"
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${TEST_USER}" --reattach_tables_before_query_execution=1 --query "${query}" 2>&1)
    REATTACH_STATUS=$?
    if [ "$mode" = ok ] && [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif [ "$mode" != ok ] && [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif [ "$mode" != ok ] && ! echo "$REATTACH_OUTPUT" | grep -q "$mode"; then
        echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$table"; then
        echo "detached"
    else
        echo "not detached"
    fi
}

echo "stored expression"
# The plain table is detached for the same user (the control), the one with a `demangle` index is not.
run_as_user ok t_reattach_sess_src "SELECT count() FROM t_reattach_sess_src FORMAT Null"
run_as_user ok t_reattach_sess_idx "SELECT count() FROM t_reattach_sess_idx SETTINGS use_skip_indexes = 0 FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_sess_idx"

echo "AI function"
run_as_user ACCESS_DENIED t_reattach_sess_src "SELECT aiGenerate(s, map('credentials', 'nc_reattach_sess_missing')) FROM t_reattach_sess_src FORMAT Null"
run_as_user ACCESS_DENIED t_reattach_sess_src "SELECT AIGenerate(s, map('credentials', 'nc_reattach_sess_missing')) FROM t_reattach_sess_src FORMAT Null"

echo "setting-supplied sources"
run_as_user ACCESS_DENIED t_reattach_sess_src "SELECT a FROM t_reattach_sess_src SETTINGS additional_table_filters = {'t_reattach_sess_src': 'a IN (SELECT a FROM ${CLICKHOUSE_DATABASE}.t_reattach_sess_secret)'} FORMAT Null"
run_as_user ACCESS_DENIED t_reattach_sess_src "SELECT a FROM t_reattach_sess_src SETTINGS additional_result_filter = 'a IN (SELECT a FROM ${CLICKHOUSE_DATABASE}.t_reattach_sess_secret)' FORMAT Null"
run_as_user UNKNOWN_TABLE t_reattach_sess_src "SELECT a FROM t_reattach_sess_src SETTINGS additional_table_filters = {'t_reattach_sess_src': 'a IN (SELECT a FROM ${CLICKHOUSE_DATABASE}.t_reattach_sess_missing)'} FORMAT Null"

echo "DEFAULT reset of the WITH scope"
# The nested `t_reattach_sess_src` resolves to the outer CTE after the reset, so the physical table is never read.
run_as_user ok t_reattach_sess_src "WITH t_reattach_sess_src AS (SELECT 1) SELECT * FROM (SELECT * FROM t_reattach_sess_src SETTINGS enable_global_with_statement = DEFAULT) SETTINGS enable_global_with_statement = 0 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_sess_idx"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_sess_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_sess_secret"
