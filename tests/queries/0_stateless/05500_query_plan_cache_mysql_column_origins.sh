#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Tag no-fasttest: requires mysql client
# The column definitions sent over the MySQL protocol report the table column each result column
# comes from. A query executed through the query plan cache must report the same origins as with
# the cache off, both on a miss and on a hit.
# The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SYSTEM DROP QUERY PLAN CACHE;
    DROP TABLE IF EXISTS t_origins;
    CREATE TABLE t_origins (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_origins VALUES (1);
"

# Prints the name, database, table and original table of every result column. The standard error is
# merged into the output and filtered with it, because the client can warn about its TLS options.
print_origins()
{
    ${MYSQL_CLIENT} --column-type-info --table --execute "$1" 2>&1 \
        | grep -E '^(Field +[0-9]+|Database|Table|Org_table):' \
        | sed -E "s/ +/ /g; s/${CLICKHOUSE_DATABASE}/<db>/"
}

QUERY="SELECT id AS x FROM t_origins"

echo "--- off"
print_origins "${QUERY}"
echo "--- miss"
print_origins "SET allow_experimental_query_plan_cache = 1; SET enable_query_plan_cache = 1; ${QUERY} SETTINGS log_comment = '05500_miss'"
echo "--- hit"
print_origins "SET allow_experimental_query_plan_cache = 1; SET enable_query_plan_cache = 1; ${QUERY} SETTINGS log_comment = '05500_hit'"

${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['QueryPlanCacheHits'] FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05500_miss', '05500_hit')
    ORDER BY event_time_microseconds;
    DROP TABLE t_origins;
"
