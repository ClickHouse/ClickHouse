#!/usr/bin/env bash
# Tags: no-fasttest

# Requires `system.query_log` to observe rows materialized by the source.
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

prefix="grants_filter_${CLICKHOUSE_DATABASE}"
cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${prefix}_alice, ${prefix}_bob"
    ${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${prefix}_role"
}
trap cleanup EXIT
cleanup
${CLICKHOUSE_CLIENT} --multiquery -q "
    CREATE USER ${prefix}_alice, ${prefix}_bob;
    CREATE ROLE ${prefix}_role;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.readable TO ${prefix}_alice;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${prefix}_bob;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.writable TO ${prefix}_role;"

read_rows()
{
    local query_id="${prefix}_$1"
    ${CLICKHOUSE_CLIENT} --query_id "$query_id" -q "
        SELECT count() FROM system.grants WHERE $2
        SETTINGS log_queries=1, log_queries_probability=1, log_queries_min_query_duration_ms=0"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT read_rows FROM system.query_log
        WHERE query_id='$query_id' AND type='QueryFinish'
            AND current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

read_rows equality "user_name='${prefix}_alice'"
read_rows conjunction "user_name='${prefix}_alice' AND database='${CLICKHOUSE_DATABASE}'"
read_rows literal_in "user_name IN ('${prefix}_alice', '${prefix}_missing', NULL)"
# The subquery contributes one additional row to the query's total `read_rows`.
read_rows subquery_in "user_name IN (SELECT '${prefix}_alice')"
read_rows disjunction "user_name='${prefix}_alice' OR role_name='${prefix}_role'"
