#!/usr/bin/env bash

# A hit of the query plan cache checks the `SELECT` privilege in the same way as the planner does on a miss:
# a subcolumn is covered by a grant on its column, and a query that selects no columns (`SELECT count()`)
# is allowed with a grant on the exact name of a subcolumn.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_column="user_column_${CLICKHOUSE_DATABASE}"
user_subcolumn="user_subcolumn_${CLICKHOUSE_DATABASE}"
# The trivial count optimization answers `SELECT count()` without a plan that can be cached.
settings="--enable_query_plan_cache=1 --enable_parallel_replicas=0 --optimize_trivial_count_query=0"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_column}, ${user_subcolumn}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (id UInt64, tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t VALUES (1, (100, 'x'))"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_column}, ${user_subcolumn} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(tup) ON ${CLICKHOUSE_DATABASE}.t TO ${user_column}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(\`tup.a\`) ON ${CLICKHOUSE_DATABASE}.t TO ${user_subcolumn}"

function run()
{
    local user=$1
    local name=$2
    local query=$3

    echo "-- ${name}: ${query}"
    ${CLICKHOUSE_CLIENT} --user "${user}" ${settings} --log_comment "${CLICKHOUSE_DATABASE}_${name}_miss" --query "${query}"
    ${CLICKHOUSE_CLIENT} --user "${user}" ${settings} --log_comment "${CLICKHOUSE_DATABASE}_${name}_hit" --query "${query}"
}

run "${user_column}" "column_grant" "SELECT tup.a FROM t"
run "${user_subcolumn}" "subcolumn_grant" "SELECT tup.a + 1 FROM t"
run "${user_subcolumn}" "trivial" "SELECT count() FROM t"

echo "-- a column without a grant is still denied on a hit"
${CLICKHOUSE_CLIENT} ${settings} --query "SELECT id FROM t"
${CLICKHOUSE_CLIENT} --user "${user_column}" ${settings} --query "SELECT id FROM t" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "-- after revoke"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(tup) ON ${CLICKHOUSE_DATABASE}.t FROM ${user_column}"
${CLICKHOUSE_CLIENT} --user "${user_column}" ${settings} --query "SELECT tup.a FROM t" 2>&1 | grep -o -m1 "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(\`tup.a\`) ON ${CLICKHOUSE_DATABASE}.t FROM ${user_subcolumn}"
${CLICKHOUSE_CLIENT} --user "${user_subcolumn}" ${settings} --query "SELECT count() FROM t" 2>&1 | grep -o -m1 "ACCESS_DENIED"

# The second run of every query is a hit, so the checks above are done on the cached plan.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['QueryPlanCacheHits']
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
        AND log_comment LIKE '${CLICKHOUSE_DATABASE}\_%'
    ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_column}, ${user_subcolumn}"
