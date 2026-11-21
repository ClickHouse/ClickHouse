#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An `INSERT` copies the entries of the manifest list of the parent snapshot into the manifest list that it writes. The
# copy reads the parent list in batches, so it has to give every entry back, in order, also for a list that has more
# entries than one batch (8192) and more than one Avro block: the parent list below has 24000 entries in two blocks of
# 13344 and 10656 entries, so that a batch ends inside a block and a block ends inside a batch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"
TABLE="t_${CLICKHOUSE_DATABASE}_parent_list_batches"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"

$CH -q "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
$CH -q "INSERT INTO ${TABLE} VALUES (1)"

PARENT_LIST=$(echo "${TABLE_PATH}"/metadata/snap-*.avro)
SNAPSHOT_ID=$($CH -q "SELECT added_snapshot_id FROM file('${PARENT_LIST}', 'Avro')")

# Replace the parent list by one with 24000 entries. The manifests they name do not exist: the `INSERT` only copies them.
$CH -q "
    INSERT INTO FUNCTION file('${PARENT_LIST}', 'Avro',
        'manifest_path String, manifest_length Int64, partition_spec_id Int32, content Int32, sequence_number Int64,
         min_sequence_number Int64, added_snapshot_id Int64, added_files_count Int32, existing_files_count Int32,
         deleted_files_count Int32, added_rows_count Int64, existing_rows_count Int64, deleted_rows_count Int64')
    SETTINGS engine_file_truncate_on_insert = 1, output_format_avro_sync_interval = 700000
    SELECT concat('metadata/manifest-', leftPad(toString(number), 6, '0'), '.avro'), 1000 + number, 0, 0, 1, 1,
        ${SNAPSHOT_ID}, 1, 0, 0, 1, 0, 0
    FROM numbers(24000)"

$CH -q "INSERT INTO ${TABLE} VALUES (2)"

NEW_LIST=
for list in "${TABLE_PATH}"/metadata/snap-*.avro
do
    [ "${list}" != "${PARENT_LIST}" ] && NEW_LIST="${list}"
done

# The copied entries, and the one entry that the `INSERT` added for its own manifest.
$CH -q "
    SELECT count(), uniqExact(manifest_path), sum(manifest_length), sum(sequence_number), sum(added_rows_count)
    FROM file('${NEW_LIST}', 'Avro') WHERE manifest_path LIKE 'metadata/manifest-%'"
$CH -q "SELECT count() FROM file('${NEW_LIST}', 'Avro') WHERE manifest_path NOT LIKE 'metadata/manifest-%'"

# The entries around the end of a batch (8191, 8192), of the first block (13343, 13344), of the second batch of the
# second block (21535, 21536) and the last one, ...
$CH -q "
    SELECT manifest_path, manifest_length FROM file('${NEW_LIST}', 'Avro')
    WHERE manifest_path IN ('metadata/manifest-008191.avro', 'metadata/manifest-008192.avro', 'metadata/manifest-013343.avro',
        'metadata/manifest-013344.avro', 'metadata/manifest-021535.avro', 'metadata/manifest-021536.avro', 'metadata/manifest-023999.avro')
    ORDER BY manifest_path"
# ... and the copied entries are in the order of the parent list.
$CH -q "
    SELECT groupArrayIf(manifest_path, manifest_path LIKE 'metadata/manifest-%')
        = arrayMap(i -> concat('metadata/manifest-', leftPad(toString(i), 6, '0'), '.avro'), range(24000))
    FROM file('${NEW_LIST}', 'Avro')
    SETTINGS max_threads = 1"

$CH -q "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
