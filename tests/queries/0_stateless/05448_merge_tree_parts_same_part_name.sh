#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source tables explicitly

# Parts from different directories may have the same name. A thread that reads one of them after
# the other must not reuse the readers of the first one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_same_name_${CLICKHOUSE_DATABASE}"
INDEX_GRANULARITY_BYTES=10485760

for i in 1 2
do
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS mtp_source_${i} SYNC;
        CREATE TABLE mtp_source_${i} (id Int64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/t${i}/'),
            index_granularity = 512,
            index_granularity_bytes = ${INDEX_GRANULARITY_BYTES},
            min_bytes_for_wide_part = 0,
            min_rows_for_wide_part = 0;
        INSERT INTO mtp_source_${i} SELECT ${i} FROM numbers(1000);"
done

# `system.parts.path` is absolute, the table function wants it relative to the root of its own disk.
PARTS=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
    SELECT arrayStringConcat(groupArray(
        part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
        || ''', marks_count = ' || toString(marks)
        || ', ranges = [(0, ' || toString(marks - 1) || ')], has_lightweight_delete = 0)'), ', ')
    FROM (SELECT * FROM system.parts
          WHERE database = currentDatabase() AND table IN ('mtp_source_1', 'mtp_source_2') AND active
          ORDER BY table)")

${CLICKHOUSE_CLIENT} --query "
    SELECT DISTINCT name FROM system.parts
    WHERE database = currentDatabase() AND table IN ('mtp_source_1', 'mtp_source_2') AND active"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(id) FROM mergeTreeParts(
        structure('id Int64'),
        parts(${PARTS}),
        disk(type = local, path = '${DISK_ROOT}/'),
        table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))
    SETTINGS max_threads = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_source_1 SYNC; DROP TABLE mtp_source_2 SYNC"
