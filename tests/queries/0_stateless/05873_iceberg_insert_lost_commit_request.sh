#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: the fail point is global, and the insert of another test could hit it.

# An insert whose write of the metadata file was not confirmed, and whose metadata file is not there, may still be
# written by the request. The insert must report the unknown outcome and keep the data files and the manifests the
# commit references, instead of removing them as if the commit was refused. The next insert commits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_writes_lose_metadata_commit_request" ||:; rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

metadata_files() { (cd "${TABLE_PATH}metadata" && ls -- *.metadata.json | sort -V | paste -sd ' '); }
count_files() { find "${TABLE_PATH}" -name "$1" | wc -l; }

manifests_before=$(count_files '*.avro')

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_writes_lose_metadata_commit_request"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (2)" 2>&1 \
    | grep -q 'The outcome of the insert commit of the Iceberg table .* is unknown' && echo "outcome unknown"

echo "data files: $(count_files '*.parquet')"
echo "manifest files kept: $(( $(count_files '*.avro') > manifests_before ))"
echo "metadata files: $(metadata_files)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (3)"
echo "metadata files: $(metadata_files)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
