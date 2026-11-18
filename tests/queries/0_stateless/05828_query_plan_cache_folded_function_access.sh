#!/usr/bin/env bash

# A function that the analyzer folds into a constant (here from an `ALIAS` column) is not visible in the AST
# and the cached plan, but it can require access rights (`demangle`). A hit of the query plan cache must check
# them for the current user, as the analysis does: for another user, and after the grant is revoked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_a="user_a_${CLICKHOUSE_DATABASE}"
user_b="user_b_${CLICKHOUSE_DATABASE}"
settings="--enable_query_plan_cache=1 --enable_parallel_replicas=0 --allow_introspection_functions=1"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_a}, ${user_b}"
${CLICKHOUSE_CLIENT} ${settings} --query "CREATE TABLE t (a UInt64, z String ALIAS demangle('_Z3foov')) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t VALUES (1)"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user_a}, ${user_b} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${user_a}, ${user_b}"
${CLICKHOUSE_CLIENT} --query "GRANT demangle ON *.* TO ${user_a}"

query="SELECT z FROM t"
${CLICKHOUSE_CLIENT} --user "${user_a}" ${settings} --log_comment "${CLICKHOUSE_DATABASE}_miss" --query "${query}"
${CLICKHOUSE_CLIENT} --user "${user_a}" ${settings} --log_comment "${CLICKHOUSE_DATABASE}_hit" --query "${query}"

echo "user without the grant"
${CLICKHOUSE_CLIENT} --user "${user_b}" ${settings} --query "${query}" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "after revoke"
${CLICKHOUSE_CLIENT} --query "REVOKE demangle ON *.* FROM ${user_a}"
${CLICKHOUSE_CLIENT} --user "${user_a}" ${settings} --query "${query}" 2>&1 | grep -o -m1 "ACCESS_DENIED"

# The query of the user with the grant is a hit, so the checks above are done on the cached plan.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['QueryPlanCacheHits']
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()
        AND log_comment IN ('${CLICKHOUSE_DATABASE}_miss', '${CLICKHOUSE_DATABASE}_hit')
    ORDER BY event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a}, ${user_b}"
