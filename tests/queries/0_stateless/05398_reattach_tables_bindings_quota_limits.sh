#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# Names on the right of `IN` that are bound to a column or a lambda argument, tables read only by the body
# of a SQL UDF, the quota of the user, and the table limits of a `CREATE` destination.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

# 1. An unqualified name on the right of `IN` is resolved as an expression first: a column of a source
# table or a lambda argument takes it, so the same-named table is never read and must not be detached.
# A qualified name can be a column of a table expression as well, so it is not detached either.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_in_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_in_rhs"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_in_src (a UInt64, t_reattach_in_rhs Array(UInt64)) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_in_rhs (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_in_src VALUES (1, [1, 2])"

check_if_not_detached "SELECT 1 IN t_reattach_in_rhs FROM t_reattach_in_src" "t_reattach_in_rhs"
check_if_detached "SELECT 1 IN t_reattach_in_rhs FROM t_reattach_in_src" "t_reattach_in_src"
check_if_not_detached "SELECT arrayMap(t_reattach_in_rhs -> 1 IN t_reattach_in_rhs, [[1, 2]])" "t_reattach_in_rhs"
check_if_not_detached "SELECT 1 IN ${CLICKHOUSE_DATABASE}.t_reattach_in_rhs" "t_reattach_in_rhs"
check_if_not_detached "SELECT 1 IN ${CLICKHOUSE_DATABASE}.t_reattach_in_rhs FROM t_reattach_in_src AS ${CLICKHOUSE_DATABASE}" "t_reattach_in_rhs"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_in_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_in_rhs"

# 2. The tables read by the body of a SQL UDF do not appear in the statement, so they escape the existence
# and access preflights. A statement calling a SQL UDF is skipped altogether: when the body reads a missing
# table, or one the user may not read, the query fails, and the table it reads directly must stay attached.
UDF_NAME="f_reattach_udf_${CLICKHOUSE_DATABASE}"
UDF_USER="user_reattach_udf_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${UDF_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${UDF_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_udf_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_udf_dim"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_udf_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE FUNCTION ${UDF_NAME} AS () -> (SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_reattach_udf_dim)"

check_fails_kind_without_detach "SELECT ${UDF_NAME}() FROM t_reattach_udf_src" "t_reattach_udf_src" "UNKNOWN_TABLE"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_udf_dim (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${UDF_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${UDF_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_udf_src TO ${UDF_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${UDF_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT ${UDF_NAME}() FROM t_reattach_udf_src" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -eq 0 ]; then
    echo "FAIL (query unexpectedly succeeded)"
elif ! echo "$REATTACH_OUTPUT" | grep -q "ACCESS_DENIED"; then
    echo "FAIL (unexpected error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_udf_src"; then
    echo "FAIL (table was detached for an access-rejected query)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "DROP USER ${UDF_USER}"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION ${UDF_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_udf_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_udf_dim"

# 3. The internal `DETACH`/`ATTACH` pair is not accounted in the quota of the user: otherwise it would
# exhaust a quota that allows the outer query, and an `ATTACH` rejected with `QUOTA_EXCEEDED` would leave
# the table detached. Only the outer query is counted: the internal pair would add two more, and the query
# reading the usage reads only a system table, which is not counted.
QUOTA_USER="user_reattach_quota_${CLICKHOUSE_DATABASE}"
QUOTA_NAME="quota_reattach_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${QUOTA_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_quota"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_quota (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${QUOTA_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO ${QUOTA_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.t_reattach_quota TO ${QUOTA_USER}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA_NAME} FOR INTERVAL 1 YEAR MAX QUERIES = 2 TO ${QUOTA_USER}"

REATTACH_OUTPUT=$(${MY_CLICKHOUSE_CLIENT} --user "${QUOTA_USER}" \
    --reattach_tables_before_query_execution=1 \
    --query "SELECT count() FROM t_reattach_quota" 2>&1)
REATTACH_STATUS=$?
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_quota"; then
    echo "OK"
else
    echo "FAIL (table was not detached)"
fi
${CLICKHOUSE_CLIENT} --user "${QUOTA_USER}" -q "SELECT queries FROM system.quota_usage"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE t_reattach_quota"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${QUOTA_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_quota"

# 4. `CREATE ... AS SELECT` with an explicit structure checks the table limit of its destination before it
# reads the `SELECT`: a destination over its limit fails the statement, and the source must stay attached.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_limit_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_limit_src (x UInt8) ENGINE = MergeTree ORDER BY x"

check_fails_kind_without_detach "SET max_temporary_tables = 1; CREATE TEMPORARY TABLE t_reattach_tmp_1 (x UInt8); CREATE TEMPORARY TABLE t_reattach_tmp_2 (x UInt8) AS SELECT x FROM t_reattach_limit_src" "t_reattach_limit_src" "TOO_MANY_TABLES"

LIMIT_DB="${CLICKHOUSE_DATABASE}_create_limit"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${LIMIT_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${LIMIT_DB} ENGINE = Atomic"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${LIMIT_DB}.t_reattach_limit_1 (x UInt8) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "ALTER DATABASE ${LIMIT_DB} MODIFY SETTING max_tables = 1"

check_fails_kind_without_detach "CREATE TABLE ${LIMIT_DB}.t_reattach_limit_2 (x UInt8) ENGINE = MergeTree ORDER BY tuple() AS SELECT x FROM t_reattach_limit_src" "t_reattach_limit_src" "TOO_MANY_TABLES"

# The control: within the limit, the source is read and reattached.
${CLICKHOUSE_CLIENT} -q "ALTER DATABASE ${LIMIT_DB} MODIFY SETTING max_tables = 2"
check_if_detached "CREATE TABLE ${LIMIT_DB}.t_reattach_limit_2 (x UInt8) ENGINE = MergeTree ORDER BY tuple() AS SELECT x FROM t_reattach_limit_src" "t_reattach_limit_src"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${LIMIT_DB}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_limit_src"
