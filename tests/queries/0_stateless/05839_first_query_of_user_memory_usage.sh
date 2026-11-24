#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The per-user state created by the first query of a user lives as long as the server, so that query must not be charged for it.
user="user_05839_${CLICKHOUSE_DATABASE}"
trap '${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"' EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH no_password"

for run in first second; do
    ${CLICKHOUSE_CLIENT} --user "${user}" --query_id "${CLICKHOUSE_DATABASE}_${run}" --max_untracked_memory 0 -q "SELECT 1 FORMAT Null"
done

${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT if(abs(toInt64(first) - toInt64(second)) < 102400, 'OK', 'first ' || toString(first) || ', second ' || toString(second))
    FROM
    (
        SELECT
            anyIf(memory_usage, query_id = '${CLICKHOUSE_DATABASE}_first') AS first,
            anyIf(memory_usage, query_id = '${CLICKHOUSE_DATABASE}_second') AS second
        FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
            AND query_id IN ('${CLICKHOUSE_DATABASE}_first', '${CLICKHOUSE_DATABASE}_second')
    )
"
