#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# `reattach_tables_before_query_execution` reattaches a table with a column of an `AggregateFunction` type that
# is gated by a setting (`enable_funnel_functions`, `allow_introspection_functions`) which is off by default,
# and resolves the sources of a refreshable materialized view in the view's own database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# 1. The types are re-parsed by the internal `ATTACH`, which must not reject them and leave the tables detached.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_tg_funnel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_tg_flame"
${CLICKHOUSE_CLIENT} --enable_funnel_functions 1 -q "CREATE TABLE t_reattach_tg_funnel (s AggregateFunction(sequenceNextNode('forward', 'head'), UInt64, String, UInt8)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 -q "CREATE TABLE t_reattach_tg_flame (s AggregateFunction(flameGraph, Array(UInt64))) ENGINE = MergeTree ORDER BY tuple()"

check_if_detached "SELECT count() FROM t_reattach_tg_funnel SETTINGS enable_funnel_functions = 0" "t_reattach_tg_funnel"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_tg_funnel"
check_if_detached "SELECT count() FROM t_reattach_tg_flame SETTINGS allow_introspection_functions = 0" "t_reattach_tg_flame"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_reattach_tg_flame"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_tg_funnel"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_tg_flame"

# 2. A refreshable materialized view reads `src` of its own database, so the same-named table of the session's
# current database stays untouched.
OTHER_DB="${CLICKHOUSE_DATABASE}_05479"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${OTHER_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${OTHER_DB} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_tg_src (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${OTHER_DB}.t_reattach_tg_src (x UInt64) ENGINE = MergeTree ORDER BY x"

check_if_detached_impl "CREATE MATERIALIZED VIEW ${OTHER_DB}.mv REFRESH EVERY 1 HOUR ENGINE = Memory AS SELECT * FROM t_reattach_tg_src" "t_reattach_tg_src"
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_tg_src"; then
    echo "FAIL (the table of the session's database was detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${OTHER_DB}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_tg_src"
