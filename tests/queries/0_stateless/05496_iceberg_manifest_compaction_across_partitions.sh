#!/usr/bin/env bash
# Tags: no-fasttest
# `OPTIMIZE TABLE ... MANIFEST` with `iceberg_insert_single_manifest_for_all_partitions` merges manifests of different
# partitions into one, keeping each file's partition and lineage. Without the setting it keeps one manifest per partition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_single"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
TABLE_PER_PARTITION="t_${CLICKHOUSE_DATABASE}_per_partition"
TABLE_PER_PARTITION_PATH="${USER_FILES_PATH}/${TABLE_PER_PARTITION}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; DROP TABLE IF EXISTS ${TABLE_PER_PARTITION}"
    rm -rf "${TABLE_PATH}" "${TABLE_PER_PARTITION_PATH}"
}
trap cleanup EXIT

latest_manifest_list()
{
    find "$1metadata" -maxdepth 1 -name 'snap-*.avro' -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-
}

# One line per live manifest: file counts and the decoded `Int64` partition bounds.
describe_manifest_list()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            added_files_count,
            existing_files_count,
            existing_rows_count,
            arrayMap(s -> (
                tupleElement(s, 'contains_null'),
                reinterpretAsInt64(tupleElement(s, 'lower_bound')),
                reinterpretAsInt64(tupleElement(s, 'upper_bound'))), partitions)
        FROM file('$(latest_manifest_list "$1")', Avro)
        ORDER BY ALL"
}

# Partition and lineage of every live data file.
files_lineage()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT file_path, partition, snapshot_id, sequence_number, record_count
        FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '$1' AND content = 'DATA'
        ORDER BY file_path"
}

# Three inserts with overlapping partitions, so three manifests that each span several partitions.
fill()
{
    local table=$1 path=$2 single=$3
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (p Int64, n Int64)
        ENGINE = IcebergLocal('${path}', 'Parquet')
        PARTITION BY p"
    for values in "(1, 10), (2, 20), (2, 21)" "(2, 22), (3, 30)" "(3, 31), (4, 40)"; do
        ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 \
            --iceberg_insert_single_manifest_for_all_partitions="${single}" \
            --query "INSERT INTO ${table} VALUES ${values}"
    done
}

compact()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --iceberg_insert_single_manifest_for_all_partitions="$2" \
        --query "OPTIMIZE TABLE $1 MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1"
}

echo "--- single manifest: before compaction"
fill "${TABLE}" "${TABLE_PATH}" 1
describe_manifest_list "${TABLE_PATH}"
BEFORE=$(files_lineage "${TABLE}")

echo "--- single manifest: after compaction"
compact "${TABLE}" 1
describe_manifest_list "${TABLE_PATH}"
AFTER=$(files_lineage "${TABLE}")
echo "$(echo "${AFTER}" | grep -c parquet) files, partitions and lineage preserved: $([ "${BEFORE}" = "${AFTER}" ] && echo yes || echo no)"
MANIFEST=$(${CLICKHOUSE_CLIENT} --query "SELECT manifest_path FROM file('$(latest_manifest_list "${TABLE_PATH}")', Avro)")
${CLICKHOUSE_CLIENT} --query "
    SELECT groupUniqArray(status), arraySort(groupArray(tupleElement(tupleElement(data_file, 'partition'), 'p')))
    FROM file('${TABLE_PATH}metadata/$(basename "${MANIFEST}")', Avro)"
for p in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT ${p}, groupArray(n) FROM (SELECT n FROM ${TABLE} WHERE p = ${p} ORDER BY n)
        SETTINGS use_iceberg_partition_pruning = 1"
done

echo "--- one manifest per partition: after compaction"
fill "${TABLE_PER_PARTITION}" "${TABLE_PER_PARTITION_PATH}" 0
BEFORE=$(files_lineage "${TABLE_PER_PARTITION}")
compact "${TABLE_PER_PARTITION}" 0
describe_manifest_list "${TABLE_PER_PARTITION_PATH}"
AFTER=$(files_lineage "${TABLE_PER_PARTITION}")
echo "$(echo "${AFTER}" | grep -c parquet) files, partitions and lineage preserved: $([ "${BEFORE}" = "${AFTER}" ] && echo yes || echo no)"
${CLICKHOUSE_CLIENT} --query "SELECT p, groupArray(n) FROM (SELECT * FROM ${TABLE_PER_PARTITION} ORDER BY n) GROUP BY p ORDER BY p"
