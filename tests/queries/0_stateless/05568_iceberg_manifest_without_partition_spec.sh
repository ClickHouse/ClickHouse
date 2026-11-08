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

# A maintenance operation that is the first to read the manifests of the table object.
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_expire"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"
cp -r "$DATA/impala/hadoop_catalog/ice/iceberg_v2_delete_equality_partitioned" "./${work_dir}/"
$CLICKHOUSE_LOCAL --session_timezone UTC --allow_insert_into_iceberg 1 --allow_expire_snapshots 1 --query "
    CREATE TABLE IF NOT EXISTS t (i Nullable(Int32), s String, d Date) ENGINE = IcebergLocal('$PWD/${work_dir}/iceberg_v2_delete_equality_partitioned');
    ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1, dry_run = 1);"
rm -rf "./${work_dir}"
