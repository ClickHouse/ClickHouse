#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_merge_manifests=1 --iceberg_insert_manifest_min_count_to_merge=3"

TABLE="t_${CLICKHOUSE_DATABASE}_merge_optimize_lineage"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

describe_current_manifests()
{
    local metadata manifest_list
    metadata=$(find "${TABLE_PATH}metadata" -maxdepth 1 -name 'v*.metadata.json' -type f | sort -V | tail -1)
    manifest_list=$(${CLICKHOUSE_CLIENT} --query "
        SELECT '${TABLE_PATH}metadata/' || splitByChar('/', JSONExtractString(snapshot, 'manifest-list'))[-1]
        FROM file('${metadata}', JSONAsString, 'json String')
        ARRAY JOIN JSONExtractArrayRaw(json, 'snapshots') AS snapshot
        WHERE JSONExtractInt(snapshot, 'snapshot-id') = JSONExtractInt(json, 'current-snapshot-id')")
    ${CLICKHOUSE_CLIENT} --query "
        SELECT content, added_files_count, existing_files_count, added_rows_count, existing_rows_count, min_sequence_number < sequence_number
        FROM file('${manifest_list}', Avro)
        ORDER BY content, sequence_number"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${TABLE_PATH}metadata/' || splitByChar('/', manifest_path)[-1], added_snapshot_id, sequence_number
        FROM file('${manifest_list}', Avro)
        WHERE content = 0" | while IFS=$'\t' read -r manifest added_snapshot_id sequence_number; do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT
                status,
                snapshot_id = ${added_snapshot_id} AS added_by_manifest_snapshot,
                sequence_number = ${sequence_number} AS manifest_sequence_number,
                tupleElement(data_file, 'record_count') AS rows,
                arrayMap(x -> tupleElement(x, 'value'), tupleElement(data_file, 'null_value_counts')) AS nulls
            FROM file('${manifest}', Avro)
            ORDER BY status, sequence_number, rows, nulls"
    done
}

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (v Nullable(Int64)) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (NULL)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (2)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (3)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DELETE WHERE v = 3"

echo "--- before OPTIMIZE"
describe_current_manifests

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${TABLE} SETTINGS allow_experimental_iceberg_compaction = 1"

echo "--- after OPTIMIZE: merged entries stay EXISTING, statistics describe each file after deletes"
describe_current_manifests
${CLICKHOUSE_CLIENT} --query "SELECT v FROM ${TABLE} ORDER BY v"
