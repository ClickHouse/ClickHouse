#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# Sources the preflights of `reattach_tables_before_query_execution` cannot see: the target of an `Alias`,
# the tables a table function resolves while it runs,
# the storages scalar functions read by name, and a column named like an unused CTE.
# Also `SYSTEM START` through an `Alias` releasing a `SYSTEM STOP` taken on its target, only with the access to it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ind_alias"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ind_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ind_dim"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ind_src (x UInt64, unused UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ind_dim (y UInt64) ENGINE = MergeTree ORDER BY y"

# 1. A qualified column named like a CTE does not reference the CTE, so the table of its definition stays attached.
check_if_not_detached "WITH unused AS (SELECT * FROM t_reattach_ind_dim) SELECT t_reattach_ind_src.unused FROM t_reattach_ind_src" "t_reattach_ind_dim"
check_if_detached "WITH used AS (SELECT * FROM t_reattach_ind_dim) SELECT t_reattach_ind_src.x FROM t_reattach_ind_src, used" "t_reattach_ind_dim"

# 2. A generator table function reads nothing, so the hook still runs; a table function that resolves
# its sources while it runs skips the hook.
check_if_detached "SELECT count() FROM t_reattach_ind_src, numbers(1) AS n" "t_reattach_ind_src"
check_if_not_detached "SELECT count() FROM t_reattach_ind_src, merge(currentDatabase(), '^t_reattach_ind_dim\$') AS m" "t_reattach_ind_src"

# 3. The read through an `Alias` checks the access to its target only when it runs, so a query referencing
# an `Alias` skips the hook: the user may read `t_reattach_ind_src` and the alias, but not its target.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ind_alias ENGINE = Alias('t_reattach_ind_dim')"
ALIAS_USER="user_reattach_ind_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ALIAS_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${ALIAS_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${ALIAS_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_ind_src TO ${ALIAS_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_reattach_ind_alias TO ${ALIAS_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${ALIAS_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT t_reattach_ind_src.x, t_reattach_ind_alias.y FROM t_reattach_ind_src CROSS JOIN t_reattach_ind_alias" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_ind_src"; then
    echo "FAIL (table was detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP USER ${ALIAS_USER}"

# 4. `SYSTEM START MERGES` through an `Alias` releases a `SYSTEM STOP MERGES` taken directly on its target.
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_reattach_ind_dim"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_ind_dim VALUES (1)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_ind_dim VALUES (2)"
${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_reattach_ind_alias"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_reattach_ind_dim FINAL"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_reattach_ind_dim' AND active"

# 5. A user with the access to the `Alias` only cannot release a `SYSTEM STOP MERGES` taken on its target.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ind_protected_alias"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ind_protected"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ind_protected (y UInt64) ENGINE = MergeTree ORDER BY y"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ind_protected_alias ENGINE = Alias('t_reattach_ind_protected')"
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_reattach_ind_protected"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_ind_protected VALUES (1)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_ind_protected VALUES (2)"
MERGES_USER="user_reattach_ind_merges_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${MERGES_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${MERGES_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM MERGES ON ${CLICKHOUSE_DATABASE}.t_reattach_ind_protected_alias TO ${MERGES_USER}"
${CLICKHOUSE_CLIENT} --user "${MERGES_USER}" -q "SYSTEM START MERGES ${CLICKHOUSE_DATABASE}.t_reattach_ind_protected_alias"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_reattach_ind_protected FINAL" 2>/dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_reattach_ind_protected' AND active"
${CLICKHOUSE_CLIENT} -q "DROP USER ${MERGES_USER}"
${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES t_reattach_ind_protected"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ind_protected_alias"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ind_protected"

# 6. `joinGet` resolves the storage named in its arguments only when it runs, so it skips the hook.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ind_join"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ind_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k)"
check_if_not_detached "SELECT joinGet('${CLICKHOUSE_DATABASE}.t_reattach_ind_join', 'v', x) FROM t_reattach_ind_src" "t_reattach_ind_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ind_join"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ind_alias"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ind_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ind_dim"
