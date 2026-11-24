#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# `reattach_tables_before_query_execution` must not detach a table whose stored definition the querying user
# cannot attach back. A column of the type `AggregateFunction(flameGraph, ...)` creates the aggregate function
# while the definition is re-parsed, and that checks the `INTROSPECTION` grant. A user without it, who can
# otherwise read the table and has the whole `DETACH`/`ATTACH` authorization, would leave the table detached.
# A query that calls `flameGraph` itself is rejected for such a user, and must not reattach its tables either.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

FG_USER="user_reattach_fg_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${FG_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_fg_state"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_fg_plain"
${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 -q "CREATE TABLE t_reattach_fg_state (s AggregateFunction(flameGraph, Array(UInt64))) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_fg_plain (trace Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${FG_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${FG_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${FG_USER}"

ADMIN_CLICKHOUSE_CLIENT="${MY_CLICKHOUSE_CLIENT}"
MY_CLICKHOUSE_CLIENT="${ADMIN_CLICKHOUSE_CLIENT} --user ${FG_USER}"

echo "without the grant: the stored definition"
check_if_not_detached "SELECT count() FROM t_reattach_fg_state" "t_reattach_fg_state"
${CLICKHOUSE_CLIENT} --user "${FG_USER}" -q "SELECT count() FROM t_reattach_fg_state"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_fg_state"

echo "without the grant: the query"
check_fails_kind_without_detach "SELECT flameGraph(trace) FROM t_reattach_fg_plain SETTINGS allow_introspection_functions = 1" "t_reattach_fg_plain" "ACCESS_DENIED"
# The control: the same user does reattach the table for a query without the function.
check_if_detached "SELECT count() FROM t_reattach_fg_plain" "t_reattach_fg_plain"

${CLICKHOUSE_CLIENT} -q "GRANT INTROSPECTION ON *.* TO ${FG_USER}"

echo "with the grant: the stored definition"
check_if_detached "SELECT count() FROM t_reattach_fg_state" "t_reattach_fg_state"
${CLICKHOUSE_CLIENT} --user "${FG_USER}" -q "SELECT count() FROM t_reattach_fg_state"

MY_CLICKHOUSE_CLIENT="${ADMIN_CLICKHOUSE_CLIENT}"

${CLICKHOUSE_CLIENT} -q "DROP USER ${FG_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_fg_state"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_fg_plain"
