#!/usr/bin/env bash
# Tags: no-fasttest

# Requires `system.query_log` to verify that mutable sets do not prune source rows.
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

prefix="grants_mutable_${CLICKHOUSE_DATABASE}"
cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${prefix}_names"
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${prefix}_alice, ${prefix}_bob"
}
trap cleanup EXIT
cleanup
${CLICKHOUSE_CLIENT} --multiquery -q "
    CREATE USER ${prefix}_alice, ${prefix}_bob;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.readable TO ${prefix}_alice;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${prefix}_bob;
    CREATE TABLE ${prefix}_names (name String) ENGINE = Set;
    INSERT INTO ${prefix}_names SELECT '${prefix}_alice';"

# A live set can grow before the final filter. Both users' rows must reach that filter.
${CLICKHOUSE_CLIENT} --query_id "${prefix}_query" -q "
    SELECT count() FROM system.grants WHERE user_name IN ${prefix}_names
    SETTINGS log_queries=1, log_queries_probability=1, log_queries_min_query_duration_ms=0"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT read_rows > 1 FROM system.query_log
    WHERE query_id='${prefix}_query' AND type='QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"
