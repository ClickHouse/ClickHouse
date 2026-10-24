#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# Unused CTE definitions, non-persistent `Set` and `Join` tables, a `Set` with a row policy in a mutation,
# and the tables read by the body of a view.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# 1. The analyzer resolves a CTE definition only when its name is used, so the tables of an unused
# definition are never read and must not be detached.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cte_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_cte_src (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_not_detached "WITH unused AS (SELECT * FROM t_reattach_cte_src) SELECT 1" "t_reattach_cte_src"
check_if_not_detached "WITH unused AS (SELECT * FROM t_reattach_cte_src), unused2 AS (SELECT * FROM unused) SELECT 1" "t_reattach_cte_src"
check_if_detached "WITH used AS (SELECT * FROM t_reattach_cte_src) SELECT * FROM used" "t_reattach_cte_src"
check_if_detached "WITH used AS (SELECT * FROM t_reattach_cte_src), used2 AS (SELECT * FROM used) SELECT * FROM used2" "t_reattach_cte_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_cte_src"

# 2. A `Set` or `Join` table with `persistent = 0` keeps its rows only in memory, and `ATTACH` restores only
# the backup files, so reattaching it would lose the rows. The persistent ones are reattached and keep them.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_join_np"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_join_p"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_set_np"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_join_np (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 0"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_join_p (k UInt64, s String) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 1"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_set_np (a UInt64) ENGINE = Set SETTINGS persistent = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_join_np VALUES (1, 'x')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_join_p VALUES (1, 'x')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_set_np VALUES (1)"

check_if_not_detached "SELECT * FROM t_reattach_join_np" "t_reattach_join_np"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_reattach_join_np"
check_if_detached "SELECT * FROM t_reattach_join_p" "t_reattach_join_p"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_reattach_join_p"
check_if_not_detached "INSERT INTO t_reattach_set_np VALUES (2)" "t_reattach_set_np"
${CLICKHOUSE_CLIENT} -q "SELECT 1 IN t_reattach_set_np, 2 IN t_reattach_set_np"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_join_np"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_join_p"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_set_np"

# 3. A mutation whose `IN` operand is a `Set` table with a row policy is rejected with `ACCESS_DENIED` before
# its interpreter reads the other tables of the predicate, which must stay attached.
POLICY_NAME="p_reattach_set_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY IF EXISTS ${POLICY_NAME} ON ${CLICKHOUSE_DATABASE}.t_reattach_policy_set"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_policy_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_policy_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_policy_set"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_policy_dst (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_policy_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_policy_set (a UInt64) ENGINE = Set"

# The control: without a row policy, the source of the predicate is reattached.
check_if_detached "ALTER TABLE t_reattach_policy_dst DELETE WHERE a IN t_reattach_policy_set AND a IN (SELECT a FROM t_reattach_policy_src)" "t_reattach_policy_src"

${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY ${POLICY_NAME} ON ${CLICKHOUSE_DATABASE}.t_reattach_policy_set USING a = 1 TO ALL"
check_fails_kind_without_detach "ALTER TABLE t_reattach_policy_dst DELETE WHERE a IN t_reattach_policy_set AND a IN (SELECT a FROM t_reattach_policy_src)" "t_reattach_policy_src" "ACCESS_DENIED"
check_fails_kind_without_detach "DELETE FROM t_reattach_policy_dst WHERE a IN t_reattach_policy_set AND a IN (SELECT a FROM t_reattach_policy_src)" "t_reattach_policy_src" "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY ${POLICY_NAME} ON ${CLICKHOUSE_DATABASE}.t_reattach_policy_set"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_policy_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_policy_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_policy_set"

# 4. The tables read by the body of a view do not appear in the statement, so they escape the existence and
# access preflights. A query referencing a view is skipped altogether: when the body reads a missing table,
# or one the caller of an `SQL SECURITY INVOKER` view may not read, the query fails, and the table it reads
# directly must stay attached.
VIEW_USER="user_reattach_view_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${VIEW_USER}"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_view_invoker"
${CLICKHOUSE_CLIENT} -q "DROP VIEW IF EXISTS t_reattach_view_missing"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_view_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_view_dim"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_view_gone"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_view_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_view_dim (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_view_gone (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE VIEW t_reattach_view_invoker SQL SECURITY INVOKER AS SELECT * FROM t_reattach_view_dim"
${CLICKHOUSE_CLIENT} -q "CREATE VIEW t_reattach_view_missing AS SELECT * FROM t_reattach_view_gone"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_view_gone"

check_fails_kind_without_detach "SELECT count() FROM t_reattach_view_src CROSS JOIN t_reattach_view_missing" "t_reattach_view_src" "UNKNOWN_TABLE"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${VIEW_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${VIEW_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_view_src TO ${VIEW_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_reattach_view_invoker TO ${VIEW_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${VIEW_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT count() FROM t_reattach_view_src CROSS JOIN t_reattach_view_invoker" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_view_src"; then
    echo "FAIL (table was detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP USER ${VIEW_USER}"
${CLICKHOUSE_CLIENT} -q "DROP VIEW t_reattach_view_invoker"
${CLICKHOUSE_CLIENT} -q "DROP VIEW t_reattach_view_missing"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_view_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_view_dim"
