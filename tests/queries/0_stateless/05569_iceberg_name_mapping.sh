#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg (Avro) support.

# Data files without field ids are read through the table property `schema.name-mapping.default`.
# `data_iceberg/name_mapping_import` was written by PyIceberg 0.11.1: `add_files` of `import_0.parquet` (nested
# fields under old names, list element named `item`) and `import_1.parquet` (`id` and `b` only), both without field
# ids, then an `append` (with field ids, `id` 4), then one metadata file per change of the mapping or the schema.
# `data_iceberg/name_mapping_partitioned` (PyIceberg 0.11.1, partitioned by `day(d)`): `add_files` of one file without
# field ids, then `d` removed from the mapping and put back, then `b` renamed to `b_old` and a new column `b` added.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The path of the table is relative, so the queries run from the directory containing it.
cd "$CUR_DIR/data_iceberg" || exit 1

# T <metadata version> [<format> [<path>]]: the table at that version.
function T()
{
    local metadata
    metadata=$(cd "$CUR_DIR/data_iceberg/name_mapping_import/metadata" && ls "$1"-*.metadata.json)
    echo "icebergLocal('${3:-name_mapping_import}', ${2:+'$2', }SETTINGS iceberg_metadata_file_path = 'metadata/$metadata')"
}

# P <metadata version>: the partitioned table at that version.
function P()
{
    local metadata
    metadata=$(cd "$CUR_DIR/data_iceberg/name_mapping_partitioned/metadata" && ls "$1"-*.metadata.json)
    echo "icebergLocal('name_mapping_partitioned', SETTINGS iceberg_metadata_file_path = 'metadata/$metadata')"
}

V_BOTH=00004             # all fields mapped, `x`, `a`, `v` also under their old names
V_UNMAPPED_B=00005       # `b` is not in the mapping
V_B_NAMELESS=00006       # `b` is in the mapping with `names: []`
V_EMPTY=00007            # the mapping is empty
V_RENAMED=00009          # `b` renamed to `b2`
V_RENAMED_UNMAPPED=00010 # `b2` is not in the mapping
V_DROPPED_MAPPED=00012   # `b2` dropped, still in the mapping
V_DROPPED_UNMAPPED=00013 # `b2` dropped, not in the mapping
V_BAD=00014              # `names` is not an array
SNAPSHOT_WITH_B=7321918627603401030
V_P_MAPPED=00001         # all fields mapped
V_P_UNMAPPED_D=00002     # `d` is not in the mapping
V_P_REASSIGNED=00005     # `b` renamed to `b_old`, the name `b` given to a new column

LAZY="query_plan_optimize_lazy_materialization = 1, query_plan_optimize_lazy_materialization_for_object_storage = 1"

$CLICKHOUSE_LOCAL -q "
SELECT 'struct, list and map fields';
SELECT id, b, s.x, l, m FROM $(T $V_BOTH) ORDER BY id;
SELECT 'not in the mapping';
SELECT id, b FROM $(T $V_UNMAPPED_B) ORDER BY id;
SELECT 'empty mapping';
SELECT count(), countIf(id IS NULL), countIf(b IS NULL) FROM $(T $V_EMPTY);
SELECT 'renamed';
SELECT id, b2 FROM $(T $V_RENAMED) ORDER BY id;
SELECT 'renamed, not in the mapping';
SELECT id, b2 FROM $(T $V_RENAMED_UNMAPPED) ORDER BY id;
SELECT 'dropped, in the mapping';
SELECT id, b FROM $(T $V_DROPPED_MAPPED) ORDER BY id SETTINGS iceberg_snapshot_id = $SNAPSHOT_WITH_B;
SELECT 'dropped, not in the mapping';
SELECT id, b FROM $(T $V_DROPPED_UNMAPPED) ORDER BY id SETTINGS iceberg_snapshot_id = $SNAPSHOT_WITH_B;
SELECT 'lazy materialization';
SELECT id, b FROM $(T $V_UNMAPPED_B) ORDER BY id LIMIT 2 SETTINGS $LAZY;
SELECT count() FROM (EXPLAIN SELECT id, b FROM $(T $V_UNMAPPED_B) ORDER BY id LIMIT 2 SETTINGS $LAZY) WHERE explain LIKE '%LazilyReadFromObjectStorage%';
SELECT 'table format ORC';
SELECT id, b FROM $(T $V_UNMAPPED_B ORC) ORDER BY id;
SELECT id, b FROM $(T $V_UNMAPPED_B ORC) ORDER BY id LIMIT 2 SETTINGS $LAZY;
SELECT count() FROM (EXPLAIN SELECT id, b FROM $(T $V_UNMAPPED_B ORC) ORDER BY id LIMIT 2 SETTINGS $LAZY) WHERE explain LIKE '%LazilyReadFromObjectStorage%';
SELECT 'manifest statistics';
SELECT count() FROM $(T $V_UNMAPPED_B) WHERE b IS NULL SETTINGS use_query_condition_cache = 0;
SELECT count() FROM $(T $V_BOTH) WHERE b > 55 SETTINGS use_query_condition_cache = 0;
SELECT 'names: []';
SELECT count(), countIf(b IS NULL) FROM $(T $V_B_NAMELESS);
SELECT count() FROM $(T $V_B_NAMELESS) WHERE b IS NULL SETTINGS use_query_condition_cache = 0;
SELECT 'partition values';
SELECT count() FROM $(P $V_P_MAPPED) WHERE d IS NULL SETTINGS use_query_condition_cache = 0;
SELECT count(), countIf(d IS NULL) FROM $(P $V_P_UNMAPPED_D);
SELECT count() FROM $(P $V_P_UNMAPPED_D) WHERE d IS NULL SETTINGS use_query_condition_cache = 0;
SELECT 'name given to a new column';
SELECT id, b_old FROM $(P $V_P_REASSIGNED) ORDER BY id;
SELECT count() FROM $(P $V_P_REASSIGNED) WHERE b_old IS NULL SETTINGS use_query_condition_cache = 0;
"

# The query above prunes the data file of `id` 4 by its manifest statistics.
$CLICKHOUSE_LOCAL --print-profile-events --profile-events-delay-ms=-1 \
    -q "SELECT count() FROM $(T $V_BOTH) WHERE b > 55 SETTINGS use_query_condition_cache = 0" 2>&1 >/dev/null \
    | grep -oE 'IcebergMinMaxIndexPrunedFiles: [1-9][0-9]*' | sed 's/: .*/ > 0/'
$CLICKHOUSE_LOCAL --print-profile-events --profile-events-delay-ms=-1 \
    -q "SELECT count() FROM $(P $V_P_MAPPED) WHERE d IS NULL SETTINGS use_query_condition_cache = 0" 2>&1 >/dev/null \
    | grep -oE 'IcebergPartitionPrunedFiles: [1-9][0-9]*' | sed 's/: .*/ > 0/'

# A query condition cache entry written under one mapping is not used under another one. On the server, because
# `clickhouse-local` has no query condition cache.
echo 'query condition cache'
CACHE_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -rf "$CACHE_DIR" "$WORK_DIR"' EXIT
rm -rf "$CACHE_DIR"
cp -r name_mapping_import "$CACHE_DIR"
for version in $V_BOTH $V_UNMAPPED_B
do
    $CLICKHOUSE_CLIENT --use_query_condition_cache=1 -q "SELECT count() FROM $(T "$version" "" "$CACHE_DIR") WHERE b IS NULL"
done

echo 'invalid mapping'
$CLICKHOUSE_LOCAL -q "SELECT * FROM $(T $V_BAD) FORMAT Null" 2>&1 | grep -o -m1 'Code: [0-9]*'

# Compaction reads the data files through the mapping and writes them with field ids. It runs only for a table with
# position deletes, and the table is copied without the later metadata files, so that `V_UNMAPPED_B` is current.
echo 'compaction'
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/t/metadata"
cp -r name_mapping_import/data "$WORK_DIR/t/"
cp name_mapping_import/metadata/*.avro "$WORK_DIR/t/metadata/"
# The ClickHouse writer names metadata files `v<N>.metadata.json`, and refuses to commit to a table with both schemes.
for i in $(seq 0 $((10#$V_UNMAPPED_B)))
do
    cp name_mapping_import/metadata/"$(printf '%05d' "$i")"-*.metadata.json "$WORK_DIR/t/metadata/v$((i + 1)).metadata.json"
done
cd "$WORK_DIR" || exit 1
$CLICKHOUSE_LOCAL -q "
CREATE TABLE t ENGINE = IcebergLocal('$WORK_DIR/t/', 'Parquet');
ALTER TABLE t DELETE WHERE id = 6 SETTINGS allow_insert_into_iceberg = 1, mutations_sync = 2;
OPTIMIZE TABLE t SETTINGS allow_experimental_iceberg_compaction = 1;
"
# The data files written by the compaction.
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$WORK_DIR/t/data/*.parquet') WHERE _file LIKE 'import%';
SELECT id, b FROM file('$WORK_DIR/t/data/*.parquet') ORDER BY id;
"
