#!/usr/bin/env bash
# `DROP VIEW ... ON CLUSTER` and `DETACH VIEW ... ON CLUSTER` are authorized by `DROP VIEW` alone,
# the same privilege the local spelling requires. They used to require `DROP TABLE` as well.
# https://github.com/ClickHouse/ClickHouse/issues/51447

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLUSTER="test_shard_localhost"
DB="${CLICKHOUSE_DATABASE}"
# Users are server-global, so the name carries the unique test name to keep this test safe against a
# concurrent copy of itself.
VIEW_DROPPER="view_dropper_05521_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -mq "
        DROP USER IF EXISTS ${VIEW_DROPPER};
        DROP VIEW IF EXISTS ${DB}.v_05521;
        DROP TABLE IF EXISTS ${DB}.t_05521;
    "
}
cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -mq "
    CREATE USER ${VIEW_DROPPER} IDENTIFIED WITH no_password;
    GRANT CREATE VIEW, DROP VIEW ON ${DB}.* TO ${VIEW_DROPPER};
    GRANT CLUSTER ON *.* TO ${VIEW_DROPPER};
    CREATE TABLE ${DB}.t_05521 (x UInt32) ENGINE = MergeTree ORDER BY x;
"

function create_view()
{
    ${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" --query "CREATE VIEW ${DB}.v_05521 AS SELECT 1"
}

echo "-- DROP VIEW, with DROP VIEW"
create_view
${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" --query "DROP VIEW ${DB}.v_05521" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --query "EXISTS VIEW ${DB}.v_05521"

echo "-- DROP VIEW ON CLUSTER, with DROP VIEW"
create_view
# The default `distributed_ddl_output_mode` waits for the task; its status table is filtered out by `grep`.
${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" \
    --query "DROP VIEW ${DB}.v_05521 ON CLUSTER ${CLUSTER}" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --query "EXISTS VIEW ${DB}.v_05521"

echo "-- DETACH VIEW ON CLUSTER, with DROP VIEW"
create_view
${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" \
    --query "DETACH VIEW ${DB}.v_05521 ON CLUSTER ${CLUSTER}" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --query "EXISTS VIEW ${DB}.v_05521"
# The short `ATTACH` form exists only as `ATTACH TABLE`, for views too.
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${DB}.v_05521"
${CLICKHOUSE_CLIENT} --query "DROP VIEW ${DB}.v_05521"

# `DROP VIEW` does not authorize dropping a table, neither directly nor through the view spelling.
echo "-- DROP TABLE ON CLUSTER of a table, with DROP VIEW only"
${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" \
    --query "DROP TABLE ${DB}.t_05521 ON CLUSTER ${CLUSTER}" 2>&1 | grep -m1 -o -F "necessary to have the grant DROP TABLE"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t_05521"

echo "-- DETACH TABLE ON CLUSTER of a table, with DROP VIEW only"
${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" \
    --query "DETACH TABLE ${DB}.t_05521 ON CLUSTER ${CLUSTER}" 2>&1 | grep -m1 -o -F "necessary to have the grant DROP TABLE"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t_05521"

echo "-- DROP VIEW ON CLUSTER of a table, with DROP VIEW only"
${CLICKHOUSE_CLIENT} --user "${VIEW_DROPPER}" \
    --query "DROP VIEW ${DB}.t_05521 ON CLUSTER ${CLUSTER}" 2>&1 | grep -m1 -o -F "is not a View"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${DB}.t_05521"
