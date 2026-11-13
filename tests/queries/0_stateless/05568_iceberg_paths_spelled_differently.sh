#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# Iceberg metadata may spell a file path differently from the table `location`: `file:/x` and `file:///x` for `/x`,
# a relative location, a file outside the table directory, or a position delete naming its data file with the
# writer's default file system (`hdfs://...`). Each spelling must name the same file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT
# `icebergLocal` of clickhouse-local accepts paths below the current directory.
cd "${WORK_DIR}" || exit 1

local_query() { ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --allow_expire_snapshots=1 -q "$1" < /dev/null; }

latest_metadata() { find "$1/warehouse/db/t/metadata" -name 'v*.metadata.json' | sort -V | tail -1; }

# $1: table directory, $2: Python code editing the metadata JSON `m`.
edit_metadata()
{
    python3 -c "
import json, sys
path = sys.argv[1]
m = json.load(open(path))
$2
json.dump(m, open(path, 'w'))
" "$(latest_metadata "$1")"
}

create_table() { local_query "CREATE TABLE t (x Int32) ENGINE = IcebergLocal('$1/warehouse/db/t'); $2"; }

# ClickHouse writes the location `/src/warehouse/db/t` and paths below it.
create_table src "INSERT INTO t VALUES (1), (2), (3);"
for dir in a a2 d; do cp -r src "${dir}"; done
create_table b "INSERT INTO t VALUES (1), (2), (3);"
# A wrong resolution into the original table cannot succeed by accident.
rm -rf src

echo "file:/// location, file:/ manifest list outside the table directory"
mkdir -p a/warehouse/shared
mv a/warehouse/db/t/metadata/snap-*.avro a/warehouse/shared/
edit_metadata a "
m['location'] = 'file:///src/warehouse/db/t'
for s in m['snapshots']:
    s['manifest-list'] = 'file:/src/warehouse/shared/' + s['manifest-list'].rsplit('/', 1)[1]
"
local_query "SELECT x FROM icebergLocal('a/warehouse/db/t') ORDER BY x"

echo "file:/ location"
edit_metadata a2 "m['location'] = 'file:/src/warehouse/db/t'"
local_query "SELECT x FROM icebergLocal('a2/warehouse/db/t') ORDER BY x"

echo "relative location, manifest list outside the table directory"
mkdir -p b/warehouse/shared
mv b/warehouse/db/t/metadata/snap-*.avro b/warehouse/shared/
edit_metadata b "
m['location'] = 'b/warehouse/db/t'
for s in m['snapshots']:
    s['manifest-list'] = 'b/warehouse/shared/' + s['manifest-list'].rsplit('/', 1)[1]
"
local_query "SELECT x FROM icebergLocal('b/warehouse/db/t') ORDER BY x"

echo "a path in another storage"
edit_metadata d "
m['location'] = 'file:///src/warehouse/db/t'
for s in m['snapshots']:
    s['manifest-list'] = 's3://bucket/x/' + s['manifest-list'].rsplit('/', 1)[1]
"
error=$(local_query "SELECT x FROM icebergLocal('d/warehouse/db/t')" 2>&1)
echo "${error}" | grep -oE '^Code: [0-9]+' | head -1
# The message names the path, the table location and the table path.
echo "${error}" | grep -q "s3://bucket/x/snap-.*file:///src/warehouse/db/t.*d/warehouse/db/t" && echo "message ok"

# $1: table directory. Prints whether the manifest bounds of `file_path` of the only position delete file are equal,
# then rewrites the file so that it names data files as `hdfs://namenode:8020/...`.
respell_deletes()
{
    local deletes manifests
    deletes=$(find "$1/warehouse/db/t/data" -name '*-deletes.parquet')
    [ "$(echo "${deletes}" | wc -l)" = 1 ] || echo "expected one position delete file"
    manifests=$(find "$1/warehouse/db/t/metadata" -name '*.avro' ! -name 'snap-*' -printf '%f\n' | sed 's/\.avro$//' | paste -sd, -)
    local_query "
        SELECT 'equal bounds', arrayFirst(b -> b.1 = 2147483546, data_file.lower_bounds).2 = arrayFirst(b -> b.1 = 2147483546, data_file.upper_bounds).2
        FROM file('$1/warehouse/db/t/metadata/{${manifests}}.avro', Avro) WHERE data_file.content = 1"
    local_query "
        INSERT INTO FUNCTION file('${deletes}.new', Parquet)
        SELECT concat('hdfs://namenode:8020', file_path) AS file_path, pos FROM file('${deletes}', Parquet)"
    mv "${deletes}.new" "${deletes}"
}

select_with_both_transforms()
{
    for roaring in 0 1; do
        ${CLICKHOUSE_LOCAL} --use_roaring_bitmap_iceberg_positional_deletes=${roaring} \
            -q "SELECT groupArray(x) FROM (SELECT x FROM icebergLocal('$1/warehouse/db/t') ORDER BY x)" < /dev/null
    done
}

echo "position deletes of one data file in the default file system"
create_table p "INSERT INTO t VALUES (1), (2), (3); ALTER TABLE t DELETE WHERE x = 2;"
respell_deletes p
select_with_both_transforms p

echo "position deletes of two data files in the default file system"
create_table p2 "INSERT INTO t VALUES (1), (2), (3); INSERT INTO t VALUES (4), (5), (6); ALTER TABLE t DELETE WHERE x IN (2, 5);"
respell_deletes p2
select_with_both_transforms p2

echo "row groups of a delete file spelled like the data paths are still pruned"
create_table r "INSERT INTO t VALUES (1), (2), (3); INSERT INTO t VALUES (4), (5), (6); ALTER TABLE t DELETE WHERE x IN (2, 5);"
deletes=$(find r/warehouse/db/t/data -name '*-deletes.parquet')
local_query "
    INSERT INTO FUNCTION file('${deletes}.new', Parquet)
    SELECT file_path, pos FROM file('${deletes}', Parquet) ORDER BY file_path, pos
    SETTINGS output_format_parquet_row_group_size = 1"
mv "${deletes}.new" "${deletes}"
${CLICKHOUSE_LOCAL} -q "
    SELECT groupArray(x) FROM (SELECT x FROM icebergLocal('r/warehouse/db/t') ORDER BY x);
    SELECT countIf(event = 'ParquetPrunedRowGroups' AND value > 0) FROM system.events" < /dev/null

echo "row groups of a delete file naming files under another top-level directory are still pruned"
create_table s "INSERT INTO t VALUES (1), (2), (3); INSERT INTO t VALUES (4), (5), (6); ALTER TABLE t DELETE WHERE x IN (2, 5);"
deletes=$(find s/warehouse/db/t/data -name '*-deletes.parquet')
local_query "
    INSERT INTO FUNCTION file('${deletes}.new', Parquet)
    SELECT file_path, pos FROM
    (
        SELECT file_path, pos FROM file('${deletes}', Parquet)
        UNION ALL SELECT '/zz/elsewhere/x.parquet', toInt64(0)
    )
    ORDER BY file_path, pos
    SETTINGS output_format_parquet_row_group_size = 1"
mv "${deletes}.new" "${deletes}"
${CLICKHOUSE_LOCAL} -q "
    SELECT groupArray(x) FROM (SELECT x FROM icebergLocal('s/warehouse/db/t') ORDER BY x);
    SELECT value FROM system.events WHERE event = 'ParquetPrunedRowGroups'" < /dev/null

echo "position deletes naming data files with a leading //"
create_table q "INSERT INTO t VALUES (1), (2), (3); INSERT INTO t VALUES (4), (5), (6); ALTER TABLE t DELETE WHERE x IN (2, 5);"
deletes=$(find q/warehouse/db/t/data -name '*-deletes.parquet')
local_query "
    INSERT INTO FUNCTION file('${deletes}.new', Parquet)
    SELECT concat('/', file_path) AS file_path, pos FROM file('${deletes}', Parquet)"
mv "${deletes}.new" "${deletes}"
select_with_both_transforms q

# Sorted by `file_path`, the positions are 2, 0, which the streaming transform cannot merge. Read in one piece, they
# select the bitmap transform. Read one row group at a time, the 0 arrives after row 2 is passed: an exception.
for row_group_size in 1000 1; do
    echo "a position delete file naming one data file in two spellings, row groups of ${row_group_size} rows"
    create_table "o${row_group_size}" "INSERT INTO t VALUES (1), (2), (3), (4); ALTER TABLE t DELETE WHERE x IN (1, 3);"
    deletes=$(find "o${row_group_size}/warehouse/db/t/data" -name '*-deletes.parquet')
    local_query "
        INSERT INTO FUNCTION file('${deletes}.new', Parquet)
        SELECT if(pos = 0, concat('file:', file_path), file_path) AS file_path, pos FROM file('${deletes}', Parquet)
        ORDER BY file_path, pos
        SETTINGS output_format_parquet_row_group_size = ${row_group_size}"
    mv "${deletes}.new" "${deletes}"
    select_with_both_transforms "o${row_group_size}" 2>&1 | grep -oE '^\[.*\]$|ICEBERG_SPECIFICATION_VIOLATION'
done

echo "expire_snapshots keeps a file a retained snapshot names in another spelling and deletes the others"
create_table x "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3);"
edit_metadata x "
current = next(s for s in m['snapshots'] if s['snapshot-id'] == m['current-snapshot-id'])
parent = next(s for s in m['snapshots'] if s['snapshot-id'] == current['parent-snapshot-id'])
parent['manifest-list'] = 'file://' + current['manifest-list']
"
local_query "
    CREATE TABLE t ENGINE = IcebergLocal('x/warehouse/db/t');
    ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);" | grep deleted_manifest_lists_count
local_query "SELECT x FROM icebergLocal('x/warehouse/db/t') ORDER BY x"
