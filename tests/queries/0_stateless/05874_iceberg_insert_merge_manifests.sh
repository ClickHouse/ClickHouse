#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_merge_manifests=1"

TABLE="t_${CLICKHOUSE_DATABASE}_merge_manifests"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
FLAT_TABLE="t_${CLICKHOUSE_DATABASE}_merge_manifests_flat"
FLAT_TABLE_PATH="${USER_FILES_PATH}/${FLAT_TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; DROP TABLE IF EXISTS ${FLAT_TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}" "${FLAT_TABLE_PATH}"
}
trap cleanup EXIT

count_manifests()
{
    find "$1metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | wc -l | tr -d ' '
}

latest_manifest_list()
{
    local metadata
    metadata=$(find "$1metadata" -maxdepth 1 -name 'v*.metadata.json' -type f | sort -V | tail -1)
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '$1metadata/' || splitByChar('/', JSONExtractString(snapshot, 'manifest-list'))[-1]
        FROM file('${metadata}', JSONAsString, 'json String')
        ARRAY JOIN JSONExtractArrayRaw(json, 'snapshots') AS snapshot
        WHERE JSONExtractInt(snapshot, 'snapshot-id') = JSONExtractInt(json, 'current-snapshot-id')"
}

describe_manifest_list()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT content, added_files_count, existing_files_count, added_rows_count, existing_rows_count, sequence_number, min_sequence_number
        FROM file('$1', Avro)
        ORDER BY content, sequence_number, added_files_count"
}

describe_merged_manifest()
{
    local manifest
    manifest=$(${CLICKHOUSE_CLIENT} --query "
        SELECT '$1metadata/' || splitByChar('/', manifest_path)[-1]
        FROM file('$(latest_manifest_list "$1")', Avro)
        WHERE content = 0 AND existing_files_count > 0")
    ${CLICKHOUSE_CLIENT} --query "
        SELECT status, sequence_number, file_sequence_number, tupleElement(data_file, 'partition') AS partition, tupleElement(data_file, 'record_count') AS rows
        FROM file('${manifest}', Avro)
        ORDER BY sequence_number NULLS LAST"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (p Int64, v Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY p"

MERGE="--iceberg_insert_manifest_min_count_to_merge=3"

${CLICKHOUSE_CLIENT} ${MERGE} --query "INSERT INTO ${TABLE} VALUES (1, 1)"
${CLICKHOUSE_CLIENT} ${MERGE} --query "INSERT INTO ${TABLE} VALUES (1, 2)"
SNAPSHOT_BEFORE_MERGE=$(${CLICKHOUSE_CLIENT} --query "
    SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}' ORDER BY made_current_at DESC LIMIT 1")

echo "--- two inserts into one partition: below the merge threshold"
describe_manifest_list "$(latest_manifest_list "${TABLE_PATH}")"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DELETE WHERE v = 2"
${CLICKHOUSE_CLIENT} ${MERGE} --query "INSERT INTO ${TABLE} VALUES (1, 3)"

echo "--- third insert merges the data manifests of the partition, the delete manifest is kept"
describe_manifest_list "$(latest_manifest_list "${TABLE_PATH}")"
describe_merged_manifest "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "SELECT p, groupArraySorted(10)(v) FROM ${TABLE} GROUP BY p ORDER BY p"

${CLICKHOUSE_CLIENT} ${MERGE} --query "INSERT INTO ${TABLE} VALUES (2, 10)"
${CLICKHOUSE_CLIENT} ${MERGE} --query "INSERT INTO ${TABLE} VALUES (1, 4)"
${CLICKHOUSE_CLIENT} ${MERGE} --query "INSERT INTO ${TABLE} VALUES (1, 5)"

echo "--- the other partition is not merged, the merged manifest is merged again"
describe_manifest_list "$(latest_manifest_list "${TABLE_PATH}")"
describe_merged_manifest "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "SELECT p, groupArraySorted(10)(v) FROM ${TABLE} GROUP BY p ORDER BY p"

echo "--- the manifests written by the merging inserts and replaced right away are removed"
count_manifests "${TABLE_PATH}"

echo "--- time travel to a snapshot before the merge"
${CLICKHOUSE_CLIENT} --query "SELECT p, v FROM ${TABLE} ORDER BY p, v SETTINGS iceberg_snapshot_id = ${SNAPSHOT_BEFORE_MERGE}"

echo "--- unpartitioned table: every insert merges into one manifest"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${FLAT_TABLE} (v Int64)
    ENGINE = IcebergLocal('${FLAT_TABLE_PATH}', 'Parquet')"
for i in 1 2 3; do
    ${CLICKHOUSE_CLIENT} --iceberg_insert_manifest_min_count_to_merge=2 --query "INSERT INTO ${FLAT_TABLE} VALUES (${i})"
done
describe_manifest_list "$(latest_manifest_list "${FLAT_TABLE_PATH}")"

echo "--- a manifest larger than the target size is not merged"
${CLICKHOUSE_CLIENT} --iceberg_insert_manifest_min_count_to_merge=2 --iceberg_insert_manifest_target_size_bytes=1 \
    --query "INSERT INTO ${FLAT_TABLE} VALUES (4)"
describe_manifest_list "$(latest_manifest_list "${FLAT_TABLE_PATH}")"

echo "--- disabled"
${CLICKHOUSE_CLIENT} --iceberg_insert_merge_manifests=0 --iceberg_insert_manifest_min_count_to_merge=2 --query "INSERT INTO ${FLAT_TABLE} VALUES (5)"
describe_manifest_list "$(latest_manifest_list "${FLAT_TABLE_PATH}")"
${CLICKHOUSE_CLIENT} --query "SELECT groupArraySorted(10)(v) FROM ${FLAT_TABLE}"
