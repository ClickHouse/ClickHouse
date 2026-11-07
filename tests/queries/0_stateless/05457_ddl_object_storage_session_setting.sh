#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: `ON CLUSTER` is not allowed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DB="ddl_object_storage_setting_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${DB}"
LOCAL_TABLE="${CLICKHOUSE_DATABASE}.ddl_object_storage_setting_local"
CLUSTER_TABLE="${CLICKHOUSE_DATABASE}.ddl_object_storage_setting_cluster"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${LOCAL_TABLE} SYNC" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
        --query "DROP TABLE IF EXISTS ${CLUSTER_TABLE} ON CLUSTER test_shard_localhost SYNC" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB} SYNC" >/dev/null 2>&1 || true
    ${CLICKHOUSE_KEEPER_CLIENT} --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true
}

cleanup
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', '{shard}', '{replica}')"

# Keep the override in the session only: a query-level `SETTINGS` clause can hide a lost `DDL` settings packet.
${CLICKHOUSE_CLIENT} --parallel_replicas_for_object_storage_tables=0 --distributed_ddl_output_mode=none --multiquery --query "
    CREATE TABLE ${LOCAL_TABLE} ENGINE = Memory
        AS SELECT getSetting('parallel_replicas_for_object_storage_tables') AS enabled;
    CREATE TABLE ${CLUSTER_TABLE} ON CLUSTER test_shard_localhost ENGINE = Memory
        AS SELECT getSetting('parallel_replicas_for_object_storage_tables') AS enabled;
    CREATE TABLE ${DB}.result ENGINE = Memory
        AS SELECT getSetting('parallel_replicas_for_object_storage_tables') AS enabled;
"

${CLICKHOUSE_CLIENT} --multiquery --query "
    SELECT 'local', enabled FROM ${LOCAL_TABLE};
    SELECT 'on_cluster', enabled FROM ${CLUSTER_TABLE};
    SELECT 'replicated', enabled FROM ${DB}.result;
"
