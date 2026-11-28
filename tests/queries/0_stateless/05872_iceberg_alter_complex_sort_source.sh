#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Iceberg needs Avro for its metadata
# Tag no-replicated-database: the replica of the other shard applies each `ALTER` again to the same table directory

# An `ALTER` that keeps every source column works on an Iceberg table sorted or partitioned by an `Array`, `Tuple` or `Map`
# column and on a table whose sort field has an unknown `source-id`. Dropping such a source column is still refused. The tables
# reopened without their own ORDER BY and PARTITION BY reach the Iceberg check, the others refuse such a DROP before it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_USER_FILES_UNIQUE}_iceberg"
rm -rf "$BASE"

query()
{
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "$1"
}

alter()
{
    query "ALTER TABLE $1" 2>&1 | grep -o -m1 -E '\(([A-Z_]+)\)$' || echo "ok"
}

query "CREATE TABLE sorted_by_array (id Int32, arr Array(Int32)) ENGINE = IcebergLocal('$BASE/sorted_by_array/') ORDER BY arr"
echo "sorted by array, add: $(alter "sorted_by_array ADD COLUMN x Nullable(Int32)")"
query "CREATE TABLE sorted_by_array_reopened ENGINE = IcebergLocal('$BASE/sorted_by_array/')"
echo "sorted by array, drop source: $(alter "sorted_by_array_reopened DROP COLUMN arr")"
query "INSERT INTO sorted_by_array VALUES (1, [2], 3)"
query "SELECT * FROM sorted_by_array ORDER BY id"

query "CREATE TABLE sorted_by_tuple (id Int32, s Tuple(a Int32, b String)) ENGINE = IcebergLocal('$BASE/sorted_by_tuple/') ORDER BY s"
echo "sorted by tuple, add: $(alter "sorted_by_tuple ADD COLUMN x Nullable(Int32)")"
query "CREATE TABLE sorted_by_tuple_reopened ENGINE = IcebergLocal('$BASE/sorted_by_tuple/')"
echo "sorted by tuple, drop source: $(alter "sorted_by_tuple_reopened DROP COLUMN s")"
echo "sorted by tuple, drop other: $(alter "sorted_by_tuple_reopened DROP COLUMN id")"

query "CREATE TABLE sorted_by_map (id Int32, m Map(String, Int32)) ENGINE = IcebergLocal('$BASE/sorted_by_map/') ORDER BY m"
echo "sorted by map, add: $(alter "sorted_by_map ADD COLUMN x Nullable(Int32)")"

query "CREATE TABLE partitioned_by_tuple (id Int32, s Tuple(a Int32, b String)) ENGINE = IcebergLocal('$BASE/partitioned_by_tuple/') PARTITION BY s"
echo "partitioned by tuple, add: $(alter "partitioned_by_tuple ADD COLUMN x Nullable(Int32)")"
query "CREATE TABLE partitioned_by_tuple_reopened ENGINE = IcebergLocal('$BASE/partitioned_by_tuple/')"
echo "partitioned by tuple, drop source: $(alter "partitioned_by_tuple_reopened DROP COLUMN s")"

query "CREATE TABLE unknown_source (id Int32) ENGINE = IcebergLocal('$BASE/unknown_source/') ORDER BY id"
python3 -c "
import json, sys
path = sys.argv[1]
metadata = json.load(open(path))
metadata['sort-orders'][0]['fields'][0]['source-id'] = 0
json.dump(metadata, open(path, 'w'))
" "$BASE/unknown_source/metadata/v1.metadata.json"
echo "unknown sort source-id, add: $(alter "unknown_source ADD COLUMN x Nullable(Int32)")"
query "INSERT INTO unknown_source VALUES (1, 2)"
query "SELECT * FROM unknown_source ORDER BY id"

query "DROP TABLE sorted_by_array; DROP TABLE sorted_by_array_reopened; DROP TABLE sorted_by_tuple; DROP TABLE sorted_by_tuple_reopened"
query "DROP TABLE sorted_by_map; DROP TABLE partitioned_by_tuple; DROP TABLE partitioned_by_tuple_reopened; DROP TABLE unknown_source"
rm -rf "$BASE"
