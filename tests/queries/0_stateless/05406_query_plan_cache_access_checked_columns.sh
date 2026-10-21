#!/usr/bin/env bash
# Test: the query plan cache tracks the columns that the planner resolves away (an `ALIAS` column
# inlined into `PREWHERE`, an `indexHint` argument) both as dependencies and for access rights.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="plan_cache_access_checked_user_${CLICKHOUSE_DATABASE}"
settings="--enable_query_plan_cache=1 --allow_experimental_analyzer=1 --enable_parallel_replicas=0"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_plan_cache_access_checked"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_plan_cache_access_checked (a UInt64, b UInt64, z UInt64 ALIAS a + 1) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_plan_cache_access_checked (a, b) VALUES (1, 10), (2, 20), (3, 30)"

echo "-- A changed ALIAS expression in PREWHERE invalidates the cached plan"
${CLICKHOUSE_CLIENT} $settings --query "SELECT count() FROM t_plan_cache_access_checked PREWHERE z > 2"
${CLICKHOUSE_CLIENT} $settings --query "SELECT count() FROM t_plan_cache_access_checked PREWHERE z > 2"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_plan_cache_access_checked MODIFY COLUMN z UInt64 ALIAS a + 2"
${CLICKHOUSE_CLIENT} $settings --query "SELECT count() FROM t_plan_cache_access_checked PREWHERE z > 2"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(a), SELECT(b), SELECT(z) ON ${CLICKHOUSE_DATABASE}.t_plan_cache_access_checked TO ${user}"

for query in \
    "SELECT count() FROM t_plan_cache_access_checked PREWHERE z > 0" \
    "SELECT count() FROM t_plan_cache_access_checked WHERE indexHint(z > 0)"
do
    echo "-- ${query}"
    ${CLICKHOUSE_CLIENT} --user "${user}" $settings --query "${query}"
    ${CLICKHOUSE_CLIENT} --user "${user}" $settings --query "${query}"
    ${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(z) ON ${CLICKHOUSE_DATABASE}.t_plan_cache_access_checked FROM ${user}"
    ${CLICKHOUSE_CLIENT} --user "${user}" $settings --query "${query}" 2>&1 | grep -o -m1 'ACCESS_DENIED'
    ${CLICKHOUSE_CLIENT} --query "GRANT SELECT(z) ON ${CLICKHOUSE_DATABASE}.t_plan_cache_access_checked TO ${user}"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_plan_cache_access_checked"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
