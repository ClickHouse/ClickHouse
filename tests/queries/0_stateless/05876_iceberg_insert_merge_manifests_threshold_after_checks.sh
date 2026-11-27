#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_merge_manifests=1 --iceberg_insert_manifest_min_count_to_merge=3"

TABLE="t_${CLICKHOUSE_DATABASE}_merge_threshold"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

describe_manifest_list()
{
    local metadata manifest_list
    metadata=$(find "${TABLE_PATH}metadata" -maxdepth 1 -name 'v*.metadata.json' -type f | sort -V | tail -1)
    manifest_list=$(${CLICKHOUSE_CLIENT} --query "
        SELECT '${TABLE_PATH}metadata/' || splitByChar('/', JSONExtractString(snapshot, 'manifest-list'))[-1]
        FROM file('${metadata}', JSONAsString, 'json String')
        ARRAY JOIN JSONExtractArrayRaw(json, 'snapshots') AS snapshot
        WHERE JSONExtractInt(snapshot, 'snapshot-id') = JSONExtractInt(json, 'current-snapshot-id')")
    ${CLICKHOUSE_CLIENT} --query "
        SELECT content, added_files_count, existing_files_count, sequence_number
        FROM file('${manifest_list}', Avro)
        ORDER BY content, sequence_number"
}

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (v Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} ADD COLUMN w Nullable(Int64)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (2, 20)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (3, 30)"

echo "--- the manifest of the old schema is not counted towards the threshold"
describe_manifest_list

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (4, 40)"

echo "--- three manifests of the current schema are merged"
describe_manifest_list
${CLICKHOUSE_CLIENT} --query "SELECT v, w FROM ${TABLE} ORDER BY v"
