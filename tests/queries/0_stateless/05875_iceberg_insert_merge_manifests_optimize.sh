#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_merge_manifests=1 --iceberg_insert_manifest_min_count_to_merge=3"

TABLE="t_${CLICKHOUSE_DATABASE}_merge_manifests_optimize"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (p Int64, v Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY p"

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1, 2)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DELETE WHERE v = 2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1, 3)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (2, 10)"

echo "--- before OPTIMIZE"
${CLICKHOUSE_CLIENT} --query "SELECT p, v FROM ${TABLE} ORDER BY p, v"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${TABLE} SETTINGS allow_experimental_iceberg_compaction = 1"

echo "--- after OPTIMIZE"
${CLICKHOUSE_CLIENT} --query "SELECT p, v FROM ${TABLE} ORDER BY p, v"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1, 4)"

echo "--- insert after OPTIMIZE"
${CLICKHOUSE_CLIENT} --query "SELECT p, v FROM ${TABLE} ORDER BY p, v"
