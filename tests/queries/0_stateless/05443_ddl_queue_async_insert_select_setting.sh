#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: `ON CLUSTER` is not allowed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="ddl_queue_async_insert_select_setting_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
        --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost" >/dev/null 2>&1 || true
}

trap cleanup EXIT

# A non-default value must be removed from the packet even when it comes from the session.
${CLICKHOUSE_CLIENT} --async_insert_select_as_async_insert=0 --join_use_nulls=1 \
    --distributed_ddl_output_mode=none \
    --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} ON CLUSTER test_shard_localhost
        (value UInt64) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        count() = 1,
        countIf(mapContains(settings, 'async_insert_select_as_async_insert')) = 0,
        countIf(settings['join_use_nulls'] = '1') = count()
    FROM system.distributed_ddl_queue
    WHERE position(query, '${CLICKHOUSE_DATABASE}.${TABLE}') > 0
        AND startsWith(query, 'CREATE TABLE')
"
