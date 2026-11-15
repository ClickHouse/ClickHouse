#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

function cleanup()
{
    rm -rf "${TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

CLIENT="${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --allow_insert_into_iceberg=1"

${CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int64) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 1"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (4), (5), (6)"
${CLIENT} --query "SELECT sum(c0) FROM ${TABLE}"

sed -E 's/"last-updated-ms" : [0-9]+/"last-updated-ms" : 9999999999999/' \
    "${TABLE_PATH}/metadata/v2.metadata.json" > "${TABLE_PATH}/metadata/v0.metadata.json"

${CLIENT} --query "SELECT sum(c0) FROM ${TABLE}"

${CLIENT} --query "DROP TABLE ${TABLE}"
