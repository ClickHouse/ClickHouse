#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# A file inside a part directory of `mergeTreeParts` on a local disk must not lead outside the root
# of the disk through a symlink, even if the part directory itself is inside the root.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_file_symlinks_${CLICKHOUSE_DATABASE}"
SOURCE_ROOT="${BASE_ROOT}/source"
READER_ROOT="${BASE_ROOT}/reader"
INDEX_GRANULARITY_BYTES=10485760

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtp_file_symlinks_source SYNC;
    CREATE TABLE mtp_file_symlinks_source (id Int64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${SOURCE_ROOT}/data/'),
        index_granularity = 512,
        index_granularity_bytes = ${INDEX_GRANULARITY_BYTES},
        min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
    INSERT INTO mtp_file_symlinks_source SELECT number FROM numbers(10);"

SOURCE_PART_PATH=$(${CLICKHOUSE_CLIENT} --query "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 'mtp_file_symlinks_source' AND active")
MARKS=$(${CLICKHOUSE_CLIENT} --query "
    SELECT marks FROM system.parts
    WHERE database = currentDatabase() AND table = 'mtp_file_symlinks_source' AND active")

function read_part()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), sum(id) FROM mergeTreeParts(
            structure('id Int64'),
            parts(Wide(path = '$1/all_1_1_0/', marks_count = ${MARKS}, ranges = [(0, $((MARKS - 1)))], has_lightweight_delete = 0)),
            disk(type = local, path = '${READER_ROOT}/'),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))" 2>&1 | grep -o "BAD_ARGUMENTS\|^[0-9]\+.*$" | head -1
}

mkdir -p "${READER_ROOT}"

echo "-- a copy of the part inside the root of the disk"
cp -r "${SOURCE_PART_PATH}" "${READER_ROOT}/copy"
mkdir -p "${READER_ROOT}/copy_part" && mv "${READER_ROOT}/copy" "${READER_ROOT}/copy_part/all_1_1_0"
read_part "copy_part"

echo "-- a part whose files are symlinks to the files inside the root of the disk"
mkdir -p "${READER_ROOT}/inside/all_1_1_0"
for f in "${READER_ROOT}/copy_part/all_1_1_0"/*; do
    ln -s "$f" "${READER_ROOT}/inside/all_1_1_0/$(basename "$f")"
done
read_part "inside"

echo "-- a part whose files are symlinks to the files outside the root of the disk"
mkdir -p "${READER_ROOT}/outside/all_1_1_0"
for f in "${SOURCE_PART_PATH%/}"/*; do
    ln -s "$f" "${READER_ROOT}/outside/all_1_1_0/$(basename "$f")"
done
read_part "outside"

echo "-- a part with a single file symlinked outside the root of the disk"
cp -r "${READER_ROOT}/copy_part" "${READER_ROOT}/one_file"
rm "${READER_ROOT}/one_file/all_1_1_0/id.bin"
ln -s "${SOURCE_PART_PATH%/}/id.bin" "${READER_ROOT}/one_file/all_1_1_0/id.bin"
read_part "one_file"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_file_symlinks_source SYNC"
rm -rf "${BASE_ROOT}"
