#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

# Equality deletes, deletion vectors (Puffin) and their corruption.
# The tables and the expected results come from the DuckDB Iceberg extension tests,
# see `data_iceberg/duckdb/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table locations are relative (`data/persistent/<table>`), so the queries run from the directory containing `data`.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1

# For metadata that has to be refused: prints `rejected` for an exception, but not for a raw Poco exception,
# a logical error, a missing file or a rejected metadata file name, which are not the reason to refuse it.
function rejected()
{
    local error
    error=$($CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -m1 'Code: ')
    if [ -z "$error" ]; then
        echo 'not rejected'
    elif grep -q -E 'Bad metadata file name|\((POCO_EXCEPTION|LOGICAL_ERROR|FILE_DOESNT_EXIST)\)' <<< "$error"; then
        echo 'unexpected error'
    else
        echo 'rejected'
    fi
}

# Written by Spark. Unpartitioned table: delete name = 'b', id = 1, (id = 3 and name = 'c'), insert, delete name = 'f'.
# Partitioned by name: delete name = 'b', (id = 3 and name = 'c'), insert, delete name = 'f'.
T="icebergLocal('data/persistent/equality_deletes/warehouse/mydb/mytable')"
P="icebergLocal('data/persistent/equality_deletes/warehouse/mydb/mytable_partitioned')"
$CLICKHOUSE_LOCAL -q "
SELECT 'equality_deletes/mytable';
SELECT * FROM $T ORDER BY ALL;
SELECT id, name FROM $T ORDER BY ALL;
SELECT bir FROM $T ORDER BY ALL;
SELECT name FROM $T ORDER BY ALL;
SELECT name, bir FROM $T ORDER BY ALL;
SELECT count() FROM $T;
SELECT count() FROM $T WHERE bir >= '2025-01-01';
SELECT bir FROM $T WHERE id = 4;
SELECT id, name FROM $T WHERE id >= 4 AND id IN (1, 4, 5, 6) ORDER BY ALL;
SELECT t.bir FROM (SELECT 1 AS x) AS v, $T AS t ORDER BY ALL;
SELECT substring(_path, 17, 16), name, id FROM $T ORDER BY ALL;

SELECT 'equality_deletes/mytable_partitioned';
SELECT * FROM $P ORDER BY ALL;
SELECT name FROM $P ORDER BY ALL;
SELECT id FROM $P ORDER BY ALL;
SELECT name, bir FROM $P ORDER BY ALL;
SELECT count() FROM $P;
SELECT substring(_path, 17, 16), name FROM $P ORDER BY ALL;
"

# An equality delete on key = 100 in partition part = 0 must not delete (part = 1, key = 100).
# An equality delete on a table partitioned by bucket(val), projecting only a non-key column.
# The snapshot with the equality delete has the id 9876543210123456789, which does not fit the type `long`
# that the Iceberg spec requires, so the metadata (`vfinal.metadata.json`) is invalid and has to be refused.
# DuckDB accepts it. Bugs metadata-file-names and metadata-validation
# (https://github.com/ClickHouse/ClickHouse/issues/130002, now POCO_EXCEPTION "Out of range: Value too large").
for t in equality_delete_cross_partition equality_delete_extra_column
do
    echo "$t $(rejected "SELECT * FROM icebergLocal('data/persistent/$t/warehouse/ns/t', SETTINGS iceberg_metadata_file_path = 'metadata/vfinal.metadata.json')")"
done
# The table before the equality delete is valid.
$CLICKHOUSE_LOCAL -q "
SELECT 'equality_delete_extra_column before the delete';
SELECT id, val FROM icebergLocal('data/persistent/equality_delete_extra_column/warehouse/ns/t', SETTINGS iceberg_metadata_file_path = 'metadata/00001-55453390-51ec-4023-a1fa-290a9ae468fa.metadata.json') ORDER BY id;
"

# A v3 table upgraded from v2: a carried-forward v2 data manifest and a v3 deletion vector written
# by old DuckDB versions as a bare blob instead of a Puffin file.
# A v3 table moved from its original location `/tmp/old_location/...`, with a deletion vector.
$CLICKHOUSE_LOCAL -q "
SELECT 'legacy_bare_deletion_vector';
SELECT id, source FROM icebergLocal('data/persistent/legacy_bare_deletion_vector/warehouse/default/legacy_bare_deletion_vector') ORDER BY id;
SELECT 'moved_deletion_vector_path';
SELECT count() FROM icebergLocal('data/persistent/moved_deletion_vector_path');
SELECT count() FROM icebergLocal('data/persistent/moved_deletion_vector_path') WHERE col = 0;
"

# The metadata of `legacy_bare_deletion_vector` with manifest lists in `deletion_vector_bitmap_count`:
# a valid deletion vector, and deletion vectors whose bitmap count is huge, negative or larger than the blob.
# Bugs metadata-file-names (`valid.metadata.json` and so on) and path-resolver (the manifests are outside of the directory of the table).
echo 'deletion_vector_bitmap_count'
$CLICKHOUSE_LOCAL -q "SELECT id, source FROM icebergLocal('data/persistent/deletion_vector_bitmap_count', SETTINGS iceberg_metadata_file_path = 'metadata/valid.metadata.json') ORDER BY id"
for c in count_huge count_negative count_truncated
do
    echo "$c $(rejected "SELECT * FROM icebergLocal('data/persistent/deletion_vector_bitmap_count', SETTINGS iceberg_metadata_file_path = 'metadata/$c.metadata.json')")"
done
