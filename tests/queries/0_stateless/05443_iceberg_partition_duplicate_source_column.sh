#!/usr/bin/env bash
# Tags: no-fasttest
# Two partition transforms of one column would produce duplicate partition field names, which makes inserts fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_dup_partition"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (dt Date) ENGINE = IcebergLocal('${TABLE_PATH}') PARTITION BY (icebergYear(dt), icebergMonth(dt))" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (dt Date) ENGINE = IcebergLocal('${TABLE_PATH}') PARTITION BY (dt, icebergBucket(4, dt))" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (dt Date, id Int64) ENGINE = IcebergLocal('${TABLE_PATH}') PARTITION BY (icebergYear(dt), icebergBucket(4, id))"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES ('2024-01-01', 1)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TABLE}"
