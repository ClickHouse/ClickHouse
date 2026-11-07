#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite
# no-fasttest: the `S3Queue` engine is not built into the fast test build

# `reattach_tables_before_query_execution` must not detach a table:
# - when a nested `SETTINGS enable_global_with_statement = 0` is dropped by a `CONST` constraint of the user,
#   so the nested name keeps resolving to the outer CTE and the physical table is never read;
# - when a `CREATE MATERIALIZED VIEW` is rejected because its inner `S3Queue` engine moves processed files to
#   a destination the user has no `WRITE` grant for.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

FREE_USER="user_reattach_scope_free_${CLICKHOUSE_DATABASE}"
CONST_USER="user_reattach_scope_const_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_scope_src"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${FREE_USER}, ${CONST_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_scope_src (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_scope_src VALUES (1)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${FREE_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${CONST_USER} IDENTIFIED WITH no_password SETTINGS enable_global_with_statement = 1 CONST"
for user in ${FREE_USER} ${CONST_USER}; do
    ${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree, TABLE ENGINE ON S3Queue TO ${user}"
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_scope_src TO ${user}"
    ${CLICKHOUSE_CLIENT} -q "GRANT CREATE VIEW ON ${CLICKHOUSE_DATABASE}.mv_reattach_scope TO ${user}"
done

# Run a query as `user` with the hook enabled and report whether `table` was detached.
# `mode` is `ok` (the query must succeed) or the error code the query must fail with.
function run_as_user()
{
    local user="$1" mode="$2" table="$3" query="$4"
    REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${user}" --reattach_tables_before_query_execution=1 --query "${query}" 2>&1)
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

echo "constrained nested WITH scope"
SCOPE_QUERY="WITH t_reattach_scope_src AS (SELECT 1) SELECT * FROM (SELECT * FROM t_reattach_scope_src SETTINGS enable_global_with_statement = 0) FORMAT Null"
# Without the constraint the nested name reads the physical table (the control).
run_as_user "${FREE_USER}" ok t_reattach_scope_src "${SCOPE_QUERY}"
# With the constraint the nested change is dropped and the name resolves to the outer CTE.
run_as_user "${CONST_USER}" ok t_reattach_scope_src "${SCOPE_QUERY}"

echo "export access of an inner engine"
# The physical table alone is detached for the same user (the control).
run_as_user "${FREE_USER}" ok t_reattach_scope_src "SELECT x FROM t_reattach_scope_src FORMAT Null"
run_as_user "${FREE_USER}" ACCESS_DENIED t_reattach_scope_src "CREATE MATERIALIZED VIEW mv_reattach_scope ENGINE = S3Queue('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_reattach_scope/', 'CSV') SETTINGS mode = 'unordered', after_processing = 'move', after_processing_move_uri = 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_reattach_scope_dst/' AS SELECT x FROM t_reattach_scope_src"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${FREE_USER}, ${CONST_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_scope_src"
