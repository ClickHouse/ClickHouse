#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: uses server-wide failpoints

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_update_pause_before_publish" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_update_pause_after_publish" 2>/dev/null
    rm -rf "${TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

CLIENT="${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --allow_insert_into_iceberg=1"

${CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int64) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} --query "INSERT INTO FUNCTION icebergLocal('${TABLE_PATH}') VALUES (1), (2), (3)"
${CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_update_pause_before_publish"
${CLIENT} --query "SELECT count() FROM ${TABLE} FORMAT Null" &
older_pid=$!
${CLIENT} --query "SYSTEM WAIT FAILPOINT iceberg_metadata_update_pause_before_publish PAUSE"

${CLIENT} --query "INSERT INTO FUNCTION icebergLocal('${TABLE_PATH}') VALUES (4), (5), (6)"

${CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_metadata_update_pause_after_publish"
${CLIENT} --query "SELECT count() FROM ${TABLE}" &
newer_pid=$!
${CLIENT} --query "SYSTEM WAIT FAILPOINT iceberg_metadata_update_pause_after_publish PAUSE"

${CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_update_pause_before_publish"
wait $older_pid
${CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_metadata_update_pause_after_publish"
wait $newer_pid

${CLIENT} --query "DROP TABLE ${TABLE}"
