#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# The part type, the number of marks and the presence of the lightweight delete mask are facts of the part
# on disk. A description that contradicts them is rejected instead of being trusted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_mismatch_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS mtp_mismatch_wide SYNC;
    DROP TABLE IF EXISTS mtp_mismatch_deleted SYNC;
    CREATE TABLE mtp_mismatch_wide (id Int64) ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/wide/'),
        index_granularity = 512,
        index_granularity_bytes = 10485760,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtp_mismatch_wide SELECT number FROM numbers(1000);
    CREATE TABLE mtp_mismatch_deleted (id Int64) ENGINE = MergeTree ORDER BY id
    SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/deleted/'),
        index_granularity = 512,
        index_granularity_bytes = 10485760,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0;
    INSERT INTO mtp_mismatch_deleted SELECT number FROM numbers(1000);
    DELETE FROM mtp_mismatch_deleted WHERE id < 100;"

# Arguments: table, part type, marks_count offset, has_lightweight_delete.
function read_part()
{
    local description
    description=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT '$2(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
            || ''', marks_count = ' || toString(marks + $3)
            || ', ranges = [(0, ' || toString(marks - 1) || ')]'
            || ', has_lightweight_delete = $4)'
        FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active")

    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM mergeTreeParts(
            structure('id Int64'),
            parts(${description}),
            disk(type = local, path = '${DISK_ROOT}/'),
            table_settings(index_granularity_bytes = 10485760))" 2>&1 | grep -o -m1 -E 'BAD_ARGUMENTS|^[0-9]+$'
}

echo "--- matching descriptions"
read_part mtp_mismatch_wide Wide 0 0
read_part mtp_mismatch_deleted Wide 0 1

echo "--- a wide part described as compact"
read_part mtp_mismatch_wide Compact 0 0

echo "--- wrong marks_count"
read_part mtp_mismatch_wide Wide 1 0

echo "--- the mask exists, but the description says it does not"
read_part mtp_mismatch_deleted Wide 0 0

echo "--- the description says the mask exists, but it does not"
read_part mtp_mismatch_wide Wide 0 1

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_mismatch_wide SYNC; DROP TABLE mtp_mismatch_deleted SYNC"
