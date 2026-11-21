#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# Dynamic subcolumns (paths of `JSON`, types of `Dynamic`) of a part read by `mergeTreeParts`
# are resolved against the serialization of the part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_dynamic_${CLICKHOUSE_DATABASE}"
INDEX_GRANULARITY_BYTES=10485760

for part_type in compact wide
do
    if [ "$part_type" = wide ]; then MIN_BYTES_FOR_WIDE_PART=0; else MIN_BYTES_FOR_WIDE_PART=1000000000; fi

    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS mtp_source SYNC;
        CREATE TABLE mtp_source (id Int64, j JSON(a.b UInt32), d Dynamic) ENGINE = MergeTree ORDER BY id
        SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/${part_type}/'),
            index_granularity = 3,
            index_granularity_bytes = ${INDEX_GRANULARITY_BYTES},
            min_bytes_for_wide_part = ${MIN_BYTES_FOR_WIDE_PART},
            min_rows_for_wide_part = 0;
        INSERT INTO mtp_source SELECT
            number,
            toJSONString(map('a', map('b', number), 'c', toString(number), 'e', [number, number + 1])),
            if(number % 2 = 0, number::Dynamic, ('s' || toString(number))::Dynamic)
        FROM numbers(10);"

    # `system.parts.path` is absolute, the table function wants it relative to the root of its own disk.
    PARTS=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT arrayStringConcat(groupArray(
            part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
            || ''', marks_count = ' || toString(marks)
            || ', ranges = [(0, ' || toString(marks - 1) || ')], has_lightweight_delete = 0)'), ', ')
        FROM system.parts WHERE database = currentDatabase() AND table = 'mtp_source' AND active")

    ${CLICKHOUSE_CLIENT} --query "SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'mtp_source' AND active"

    ${CLICKHOUSE_CLIENT} --query "
        SELECT id, j.a.b, j.c, j.e, j.missing, d.UInt64, d.String, dynamicType(d)
        FROM mergeTreeParts(
            structure('id Int64, j JSON(a.b UInt32), d Dynamic'),
            parts(${PARTS}),
            disk(type = local, path = '${DISK_ROOT}/'),
            table_settings(index_granularity_bytes = ${INDEX_GRANULARITY_BYTES}))
        ORDER BY id"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_source SYNC"
