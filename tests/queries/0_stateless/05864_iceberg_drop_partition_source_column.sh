#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Iceberg needs Avro for its metadata
# Tag no-replicated-database: the replica of the other shard applies each `ALTER` again to the same table directory

# `ALTER TABLE ... DROP COLUMN` must not drop a source column of the default partition spec or sort order of an Iceberg table:
# the committed metadata was invalid for other Iceberg engines and every later INSERT failed. A `void` partition field and
# an older partition spec keep no source column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_USER_FILES_UNIQUE}_iceberg"
rm -rf "$BASE"
mkdir -p "$BASE"
for table in v1_partition_evolution_void v2_partition_evolution
do
    cp -r "$CUR_DIR/data_lake_corpus/iceberg/$table" "$BASE/$table"
done

query()
{
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "$1"
}

drop()
{
    query "ALTER TABLE $1 DROP COLUMN $2" 2>&1 | grep -o -m1 -E '\(([A-Z_]+)\)$' || echo "dropped"
}

metadata_files()
{
    find "$1/metadata" -name '*.metadata.json' | wc -l
}

query "CREATE TABLE t (id Int32, p String, s Tuple(a Int32, b String)) ENGINE = IcebergLocal('$BASE/t/') PARTITION BY p ORDER BY s.a SETTINGS iceberg_format_version = 2"
query "INSERT INTO t VALUES (1, 'a', (1, 'x'))"
# Opened on the directory, without its own PARTITION BY and ORDER BY. The ALTER loads the sort order, whose key refuses dropping `s`.
query "CREATE TABLE t2 ENGINE = IcebergLocal('$BASE/t/')"
before=$(metadata_files "$BASE/t")
echo "partition source p: $(drop t2 p)"
echo "sort source s: $(drop t2 s)"
echo "unchanged: $([ "$before" = "$(metadata_files "$BASE/t")" ] && echo 1 || echo 0)"
echo "other column id: $(drop t2 id)"
query "INSERT INTO t2 VALUES ('b', (2, 'y'))"
query "SELECT * FROM t2 ORDER BY p"

query "CREATE TABLE v1 ENGINE = IcebergLocal('$BASE/v1_partition_evolution_void/')"
echo "v1 truncate source s: $(drop v1 s)"
echo "v1 void source p: $(drop v1 p)"

query "CREATE TABLE v2 ENGINE = IcebergLocal('$BASE/v2_partition_evolution/')"
echo "v2 bucket source id: $(drop v2 id)"
echo "v2 older spec source p: $(drop v2 p)"

query "DROP TABLE t; DROP TABLE t2; DROP TABLE v1; DROP TABLE v2"
rm -rf "$BASE"
