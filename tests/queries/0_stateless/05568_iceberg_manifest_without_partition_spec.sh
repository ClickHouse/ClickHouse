#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# Iceberg tables whose manifests and manifest lists have no `partition-spec`, `schema` and `format-version` keys in their
# Avro metadata, because they were rewritten with a plain Avro writer. The tables are copied unchanged from the Apache
# Impala test data (https://github.com/apache/impala/tree/e35eb8f3582449d736e74ce32f1138f5f5dc085b/testdata/data/iceberg_test)
# and the Dremio test suite (https://github.com/dremio/dremio-oss/tree/799ccbda47e6f2e1bfacf1ccbded174e00d4150a/sabot/kernel/src/test/resources/iceberg),
# both under the Apache License 2.0.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg"

q()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1"
}

# Equality deletes, applied within the partition of the spec named by the manifest list.
q "SELECT * FROM icebergLocal('$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned') ORDER BY ALL"

# All manifests are written with spec 0 (`d`), while the default spec of the table is 1 (`d`, `i`).
q "SELECT * FROM icebergLocal('$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partition_evolution') ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partition_evolution') WHERE i = 2 ORDER BY ALL SETTINGS use_iceberg_partition_pruning = 1"

# One of the manifests has only `dir0` in its partition tuple; the spec field `dir1` is NULL there.
q "CREATE TABLE t ENGINE = IcebergLocal('$DATA/dremio/internal_metadata_inconsistent_partitions'); SELECT partition FROM system.iceberg_files WHERE table = 't' ORDER BY partition"

# Row lineage of a format version 3 table.
q "SELECT count(), min(_row_id), max(_row_id), uniqExact(_row_id), min(_last_updated_sequence_number), max(_last_updated_sequence_number) FROM icebergLocal('$DATA/impala/iceberg_v3/trino_variant')"
q "SELECT _row_id FROM icebergLocal('$DATA/impala/iceberg_v3/trino_variant') WHERE id = 62"

# The spec named by the manifest list is not in the table metadata. `IcebergLocal` accepts only paths inside the working directory.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_unknown_spec"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
cp -r "$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned" "./${work_dir}/"
sed -i -e 's/"default-spec-id" : 0/"default-spec-id" : 7/' -e 's/"spec-id" : 0/"spec-id" : 7/' "./${work_dir}/iceberg_v2_delete_equality_partitioned/metadata/v5.metadata.json"
q "SELECT * FROM icebergLocal('$PWD/${work_dir}/iceberg_v2_delete_equality_partitioned')" 2>&1 | grep -o -m1 -e 'no partition spec with id 0' -e 'ICEBERG_SPECIFICATION_VIOLATION'
rm -rf "./${work_dir}"

# The field id of the partition record is not in the spec named by the manifest list.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_field_id"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
cp -r "$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned" "./${work_dir}/"
sed -i -e 's/"field-id" : 1000/"field-id" : 1005/' "./${work_dir}/iceberg_v2_delete_equality_partitioned/metadata/v5.metadata.json"
q "SELECT * FROM icebergLocal('$PWD/${work_dir}/iceberg_v2_delete_equality_partitioned')" 2>&1 | grep -o -m1 -e 'cannot be matched to the partition spec with id 0' -e 'ICEBERG_SPECIFICATION_VIOLATION'
rm -rf "./${work_dir}"

# Partition values are matched to the spec fields by field id: the Dremio table with its spec fields reversed. A partition
# record without field ids is read by position: the Dremio table with the field ids removed from its two-field records.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_record_order"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
cp -r "$DATA/dremio/internal_metadata_inconsistent_partitions" "./${work_dir}/reversed_spec"
cp -r "$DATA/dremio/internal_metadata_inconsistent_partitions" "./${work_dir}/no_field_ids"
python3 -c 'import json, sys; p = sys.argv[1]; m = json.load(open(p)); m["partition-specs"][0]["fields"].reverse(); json.dump(m, open(p, "w"))' "./${work_dir}"/reversed_spec/metadata/*.metadata.json
for manifest in "./${work_dir}"/no_field_ids/metadata/*.avro; do
    python3 -c 'import sys; p = sys.argv[1]; d = open(p, "rb").read()
if b"\"field-id\":1001}" in d and b"manifest_entry" in d:
    open(p, "wb").write(d.replace(b"\"field-id\":1000}", b"\"field-iX\":1000}").replace(b"\"field-id\":1001}", b"\"field-iX\":1001}"))' "$manifest"
done
for t in reversed_spec no_field_ids; do
    q "CREATE TABLE t ENGINE = IcebergLocal('$PWD/${work_dir}/$t'); SELECT partition FROM system.iceberg_files WHERE table = 't' ORDER BY partition"
done
rm -rf "./${work_dir}"

# A field id that is not an integer is refused, in the partition record and in the partition spec.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_invalid_field_id"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
cp -r "$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned" "./${work_dir}/record"
cp -r "$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned" "./${work_dir}/spec"
for manifest in "./${work_dir}"/record/metadata/*.avro; do
    python3 -c 'import sys; p = sys.argv[1]; d = open(p, "rb").read(); open(p, "wb").write(d.replace(b"\"field-id\":1000}", b"\"field-id\":\"10\"}"))' "$manifest"
done
sed -i -e 's/"field-id" : 1000/"field-id" : "1000"/' "./${work_dir}/spec/metadata/v5.metadata.json"
for t in record spec; do
    q "SELECT * FROM icebergLocal('$PWD/${work_dir}/$t')" 2>&1 | grep -o -m1 -e "field ids of the partition $t are incomplete, invalid or not unique" -e 'ICEBERG_SPECIFICATION_VIOLATION'
done
rm -rf "./${work_dir}"

# A maintenance operation that is the first to read the manifests of the table object.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_expire"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
cp -r "$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned" "./${work_dir}/"
$CLICKHOUSE_LOCAL --session_timezone UTC --allow_insert_into_iceberg 1 --allow_expire_snapshots 1 --query "
    CREATE TABLE IF NOT EXISTS t (i Nullable(Int32), s String, d Date) ENGINE = IcebergLocal('$PWD/${work_dir}/iceberg_v2_delete_equality_partitioned');
    ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1, dry_run = 1);"
rm -rf "./${work_dir}"

# Manifests without `schema` around a column rename. The schema of an ORC file is known from the `schema-id` of its manifest
# or from the snapshot that added it. Without both, the reader refuses rather than assuming the current schema.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_renamed"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
table="$PWD/${work_dir}/t"
ch="$CLICKHOUSE_LOCAL --allow_insert_into_iceberg 1 --allow_expire_snapshots 1"
$ch --query "CREATE TABLE t (a Int32) ENGINE = IcebergLocal('$table', 'ORC'); INSERT INTO t VALUES (7); INSERT INTO t VALUES (9);"
$ch --query "CREATE TABLE t ENGINE = IcebergLocal('$table', 'ORC'); ALTER TABLE t RENAME COLUMN a TO b;"
$ch --query "CREATE TABLE t ENGINE = IcebergLocal('$table', 'ORC'); INSERT INTO t VALUES (8);"
cp -r "$table" "${table}_rewritten"
# Byte replacements of the same length in the Avro header of every manifest: python3 -c '<statement>' <table path>.
patch_manifests()
{
    for manifest in "$2"/metadata/*.avro; do
        [[ "$(basename "$manifest")" == snap-* ]] && continue
        python3 -c "import sys; p = sys.argv[1]; d = open(p, 'rb').read(); $1; open(p, 'wb').write(d)" "$manifest"
    done
}
hide_schema='d = d.replace(b"\x0cschema", b"\x0cschemX", 1)'
hide_schema_id='d = d.replace(b"\x12schema-id", b"\x12schemX-id", 1)'
patch_manifests "$hide_schema" "$table"
q "SELECT b FROM icebergLocal('$table') ORDER BY b"
$ch --query "CREATE TABLE t ENGINE = IcebergLocal('$table', 'ORC'); ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);" > /dev/null
q "SELECT b FROM icebergLocal('$table') ORDER BY b"
patch_manifests "$hide_schema_id" "$table"
q "SELECT b FROM icebergLocal('$table') ORDER BY b" 2>&1 | grep -o -m1 -e 'other than its initial schema 0' -e 'ICEBERG_SPECIFICATION_VIOLATION'
# Java's expiry with `clean_expired_metadata` also drops the schemas no retained snapshot uses, leaving only schema 1.
python3 -c 'import glob, json, os, re, sys
p = max(glob.glob(sys.argv[1] + "/metadata/v*.metadata.json"), key=lambda f: int(re.match(r"v(\d+)", os.path.basename(f)).group(1)))
m = json.load(open(p)); m["schemas"] = [s for s in m["schemas"] if s["schema-id"] == m["current-schema-id"]]; json.dump(m, open(p, "w"))' "$table"
q "SELECT b FROM icebergLocal('$table') ORDER BY b" 2>&1 | grep -o -m1 -e 'other than its initial schema 0' -e 'ICEBERG_SPECIFICATION_VIOLATION'
# A rewritten manifest lists as EXISTING the files of other snapshots. Java writes it with the current `schema-id` (1 here),
# and neither that nor the snapshot that added it gives their schema, so once their own snapshots are expired the reader
# refuses them.
$ch --allow_experimental_iceberg_compaction 1 --query "CREATE TABLE t ENGINE = IcebergLocal('${table}_rewritten', 'ORC');
    OPTIMIZE TABLE t MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 0;"
patch_manifests "$hide_schema; d = d.replace(b'\x12schema-id\x020', b'\x12schema-id\x021', 1)" "${table}_rewritten"
q "SELECT b FROM icebergLocal('${table}_rewritten') ORDER BY b"
$ch --query "CREATE TABLE t ENGINE = IcebergLocal('${table}_rewritten', 'ORC'); ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);" > /dev/null
q "SELECT b FROM icebergLocal('${table}_rewritten') ORDER BY b" 2>&1 | grep -o -m1 -e 'the schema of that data file is unknown' -e 'ICEBERG_SPECIFICATION_VIOLATION'
patch_manifests "$hide_schema_id" "${table}_rewritten"
q "SELECT b FROM icebergLocal('${table}_rewritten') ORDER BY b" 2>&1 | grep -o -m1 -e 'the schema of that data file is unknown' -e 'ICEBERG_SPECIFICATION_VIOLATION'
rm -rf "./${work_dir}"
