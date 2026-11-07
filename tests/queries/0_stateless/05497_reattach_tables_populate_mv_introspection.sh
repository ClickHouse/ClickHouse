#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# `reattach_tables_before_query_execution` must keep an access-rejected query side-effect free: a
# `CREATE MATERIALIZED VIEW ... POPULATE` without `INSERT` on the view, and a query calling an introspection
# function (which checks a grant of its own when it is created).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

TEST_USER="user_reattach_pmi_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_pmi_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_pmi_src"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_pmi_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_pmi_src VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${TEST_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE VIEW ON ${CLICKHOUSE_DATABASE}.t_reattach_pmi_mv TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_pmi_src TO ${TEST_USER}"

# Run a query as the test user with the hook enabled; expect either an access error without the source
# being detached ("denied") or success with the source detached ("granted").
function run_as_user()
{
    local mode="$1" query="$2"
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${TEST_USER}" --reattach_tables_before_query_execution=1 --query "${query}" 2>&1)
    REATTACH_STATUS=$?
    if [ "$mode" = denied ]; then
        if [ "$REATTACH_STATUS" -eq 0 ]; then
            echo "FAIL (query unexpectedly succeeded)"
        elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
            echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
        elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_pmi_src"; then
            echo "FAIL (source was detached for an access-rejected query)"
        else
            echo "OK"
        fi
    else
        if [ "$REATTACH_STATUS" -ne 0 ]; then
            echo "FAIL (client error: $REATTACH_OUTPUT)"
        elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_pmi_src"; then
            echo "OK"
        else
            echo "FAIL (source was not detached)"
        fi
    fi
}

# 1. A populating materialized view inserts into the view itself, which needs `INSERT` on it.
for atomically in 0 1
do
    MV_QUERY="CREATE MATERIALIZED VIEW t_reattach_pmi_mv ENGINE = MergeTree ORDER BY a POPULATE AS SELECT a FROM t_reattach_pmi_src SETTINGS materialized_views_populate_atomically = ${atomically}"
    run_as_user denied "${MV_QUERY}"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_pmi_mv"
done

# The granted control detaches the source and populates the view.
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_reattach_pmi_mv TO ${TEST_USER}"
run_as_user granted "${MV_QUERY}"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_reattach_pmi_mv"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_pmi_mv"

# 2. Introspection functions check their own grant when they are created, which no preflight sees.
run_as_user granted "SELECT a FROM t_reattach_pmi_src FORMAT Null"
for function in "demangle(toString(a))" "addressToSymbol(a)" "addressToLine(a)" "addressToLineWithInlines(a)"
do
    run_as_user denied "SELECT ${function} FROM t_reattach_pmi_src SETTINGS allow_introspection_functions = 1 FORMAT Null"
done
# With the grant, the query is still skipped by the hook, and it succeeds.
${CLICKHOUSE_CLIENT} -q "GRANT INTROSPECTION ON *.* TO ${TEST_USER}"
for function in "demangle(toString(a))" "addressToSymbol(a)" "addressToLine(a)" "addressToLineWithInlines(a)"
do
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${TEST_USER}" --reattach_tables_before_query_execution=1 --allow_introspection_functions=1 --query "SELECT ${function} FROM t_reattach_pmi_src FORMAT Null" 2>&1)
    REATTACH_STATUS=$?
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_pmi_src"; then
        echo "FAIL (source was detached)"
    else
        echo "OK"
    fi
done

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_pmi_src"
