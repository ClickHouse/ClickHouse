#!/usr/bin/env bash
# Tags: no-fasttest

# One INSERT into a partitioned Iceberg table writes one manifest for all the partitions it touches, like Spark and
# PyIceberg, instead of one manifest per partition. The manifest-list entry must then describe the whole manifest:
# real file/row counts and a `partitions` summary whose bounds span every partition tuple, with `contains_null` when
# one of them is NULL. Partition pruning keeps working on such a manifest, and DROP PARTITION, which used to refuse a
# manifest shared with other partitions, rewrites it without the dropped files while keeping the others' lineage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_single_manifest"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
NULL_TABLE="t_${CLICKHOUSE_DATABASE}_null_partition"
NULL_TABLE_PATH="${USER_FILES_PATH}/${NULL_TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; DROP TABLE IF EXISTS ${NULL_TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}" "${NULL_TABLE_PATH}"
}
trap cleanup EXIT

# Small data files so one partition spans several files and the manifest holds more entries than partitions.
ROLLOVER="iceberg_insert_max_rows_in_data_file = 2, max_insert_threads = 1, max_block_size = 2,
          max_insert_block_size = 2, min_insert_block_size_rows = 2, min_insert_block_size_bytes = 0"

count_manifests()
{
    find "$1metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | wc -l | tr -d ' '
}

latest_manifest_list()
{
    find "$1metadata" -maxdepth 1 -name 'snap-*.avro' -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-
}

# One line per manifest-list entry: counts and the decoded Int64 partition bounds.
describe_manifest_list()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            added_files_count,
            existing_files_count,
            added_rows_count,
            existing_rows_count,
            arrayMap(s -> (
                tupleElement(s, 'contains_null'),
                reinterpretAsInt64(tupleElement(s, 'lower_bound')),
                reinterpretAsInt64(tupleElement(s, 'upper_bound'))), partitions) AS partition_summary
        FROM file('$1', Avro)
        ORDER BY added_files_count, existing_files_count"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (p Int64, v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY p"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} SELECT number % 3 + 10, toString(number) FROM numbers(12) SETTINGS ${ROLLOVER}"

echo "--- one INSERT, three partitions: manifests written"
count_manifests "${TABLE_PATH}"

echo "--- manifest-list entry: files, rows, partition bounds"
describe_manifest_list "$(latest_manifest_list "${TABLE_PATH}")"

echo "--- manifest entries: own partition value, status ADDED, inherited sequence number"
MANIFEST=$(find "${TABLE_PATH}metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f)
${CLICKHOUSE_CLIENT} --query "
    SELECT tupleElement(data_file, 'partition') AS partition, status, sequence_number, tupleElement(data_file, 'record_count') AS rows
    FROM file('${MANIFEST}', Avro)
    ORDER BY partition, rows"

echo "--- rows read back"
${CLICKHOUSE_CLIENT} --query "SELECT p, count(), sum(toInt64(v)) FROM ${TABLE} GROUP BY p ORDER BY p"

echo "--- partition pruning on the shared manifest"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TABLE} WHERE p = 11
        SETTINGS use_iceberg_partition_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_prune_shared';
    SELECT count() FROM ${TABLE} WHERE p = 99
        SETTINGS use_iceberg_partition_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_prune_shared';
    SYSTEM FLUSH LOGS query_log;"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['IcebergPartitionPrunedFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment = '${CLICKHOUSE_DATABASE}_prune_shared'
    ORDER BY event_time_microseconds
    SETTINGS enable_parallel_replicas = 0"

# The first manifest is carried forward untouched; the second insert gets its own.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (10, '100'), (12, '120')"
echo "--- second INSERT: manifests written"
count_manifests "${TABLE_PATH}"
describe_manifest_list "$(latest_manifest_list "${TABLE_PATH}")"

SNAPSHOT_BEFORE_DROP=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
    SELECT snapshot_id FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at DESC LIMIT 1")

echo "--- DROP PARTITION 11: the manifest shared with partitions 10 and 12 is rewritten, the other one is carried forward"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 11"
${CLICKHOUSE_CLIENT} --query "SELECT p, count(), sum(toInt64(v)) FROM ${TABLE} GROUP BY p ORDER BY p"
describe_manifest_list "$(latest_manifest_list "${TABLE_PATH}")"

echo "--- rewritten manifest keeps the files as EXISTING with their original sequence number"
NEW_MANIFESTS=$(${CLICKHOUSE_CLIENT} --query "SELECT manifest_path FROM file('$(latest_manifest_list "${TABLE_PATH}")', Avro) ORDER BY added_files_count DESC, existing_files_count DESC")
for manifest in ${NEW_MANIFESTS}; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT tupleElement(data_file, 'partition') AS partition, status, sequence_number, tupleElement(data_file, 'record_count') AS rows
        FROM file('${TABLE_PATH}metadata/$(basename "${manifest}")', Avro)
        ORDER BY partition, rows"
done

echo "--- time travel to before the DROP still sees partition 11"
${CLICKHOUSE_CLIENT} --iceberg_snapshot_id="${SNAPSHOT_BEFORE_DROP}" --query "SELECT p, count() FROM ${TABLE} GROUP BY p ORDER BY p"

echo "--- DELETE in partition 10 then DROP PARTITION 10 (data manifest shared with 12, delete manifest fully matched)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "DELETE FROM ${TABLE} WHERE p = 10 AND v = '0'"
${CLICKHOUSE_CLIENT} --query "SELECT p, count(), sum(toInt64(v)) FROM ${TABLE} GROUP BY p ORDER BY p"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 10"
${CLICKHOUSE_CLIENT} --query "SELECT p, count(), sum(toInt64(v)) FROM ${TABLE} GROUP BY p ORDER BY p"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 12"
echo "--- all partitions dropped"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

echo "--- NULL and non-NULL partition values in one INSERT"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${NULL_TABLE} (p Nullable(Int64), v Int64)
    ENGINE = IcebergLocal('${NULL_TABLE_PATH}', 'Parquet')
    PARTITION BY p"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${NULL_TABLE} VALUES (NULL, 1), (5, 2), (NULL, 3), (-7, 4)"
count_manifests "${NULL_TABLE_PATH}"
describe_manifest_list "$(latest_manifest_list "${NULL_TABLE_PATH}")"
${CLICKHOUSE_CLIENT} --query "SELECT p, sum(v) FROM ${NULL_TABLE} GROUP BY p ORDER BY p NULLS FIRST"
${CLICKHOUSE_CLIENT} --query "SELECT sum(v) FROM ${NULL_TABLE} WHERE p IS NULL SETTINGS use_iceberg_partition_pruning = 1"
${CLICKHOUSE_CLIENT} --query "SELECT sum(v) FROM ${NULL_TABLE} WHERE p = -7 SETTINGS use_iceberg_partition_pruning = 1"
