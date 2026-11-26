#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: the fail point is global, and the insert of another test could hit it.

# An insert whose metadata file was written, but whose response was lost, is published. The insert must recognize its
# own snapshot in the attempted metadata file and succeed, instead of removing the files it references and retrying.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_writes_lose_metadata_commit_response" ||:; rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_writes_lose_metadata_commit_response"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2)"

echo "metadata files: $(cd "${TABLE_PATH}metadata" && ls -- *.metadata.json | sort -V | paste -sd ' ')"
echo "data files: $(find "${TABLE_PATH}" -name '*.parquet' | wc -l)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
