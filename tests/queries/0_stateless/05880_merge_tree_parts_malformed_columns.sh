#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
# no-shared-merge-tree: custom disk
# no-random-merge-tree-settings: the table function is given the granularity of the source table explicitly

# `mergeTreeParts` builds its reader from the files of the part, so a part whose `columns.txt` lists no
# columns, or lists other columns than `columns_substreams.txt` describes, has to be reported as
# corrupted, the same way a table reports it when it loads the part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_malformed_${CLICKHOUSE_DATABASE}"

function create_table()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS mtp_malformed_$1 SYNC;
        CREATE TABLE mtp_malformed_$1 (id Int64) ENGINE = MergeTree ORDER BY id
        SETTINGS disk = disk(type = local, path = '${DISK_ROOT}/$1/'),
            index_granularity = 512,
            index_granularity_bytes = 10485760,
            min_rows_for_wide_part = $2,
            min_bytes_for_wide_part = $2;
        INSERT INTO mtp_malformed_$1 SELECT number FROM numbers(1000);"
}

function part_path()
{
    ${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'mtp_malformed_$1' AND active"
}

# Arguments: table, the query to run over the part.
function read_part()
{
    local description
    description=$(${CLICKHOUSE_CLIENT} --format TSVRaw --query "
        SELECT part_type || '(path = ''' || replaceOne(path, '${DISK_ROOT}/', '')
            || ''', marks_count = ' || toString(marks)
            || ', ranges = [(0, ' || toString(marks - 1) || ')]'
            || ', has_lightweight_delete = 0)'
        FROM system.parts WHERE database = currentDatabase() AND table = 'mtp_malformed_$1' AND active")

    ${CLICKHOUSE_CLIENT} --query "
        SELECT $2 FROM mergeTreeParts(
            structure('id Int64'),
            parts(${description}),
            disk(type = local, path = '${DISK_ROOT}/'),
            table_settings(index_granularity_bytes = 10485760))" 2>&1 | grep -o -m1 -E 'CORRUPTED_DATA|^[0-9]+$'
}

create_table compact 1000000000
create_table wide 0

echo "--- intact parts"
read_part compact "count()"
read_part compact "sum(id)"
read_part wide "count()"
read_part wide "sum(id)"

for kind in compact wide
do
    path=$(part_path ${kind})
    cp "${path}columns.txt" "${path}columns.txt.bak"

    echo "--- ${kind}: columns.txt without columns"
    printf 'columns format version: 1\n0 columns:\n' > "${path}columns.txt"
    read_part ${kind} "count()"
    read_part ${kind} "sum(id)"

    echo "--- ${kind}: columns.txt with other columns than columns_substreams.txt"
    printf 'columns format version: 1\n2 columns:\n`id` Int64\n`extra` Int64\n' > "${path}columns.txt"
    read_part ${kind} "count()"
    read_part ${kind} "sum(id)"

    mv "${path}columns.txt.bak" "${path}columns.txt"
done

echo "--- restored parts"
read_part compact "sum(id)"
read_part wide "sum(id)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mtp_malformed_compact SYNC; DROP TABLE mtp_malformed_wide SYNC"
