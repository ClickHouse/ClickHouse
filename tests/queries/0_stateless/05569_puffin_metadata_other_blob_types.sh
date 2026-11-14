#!/usr/bin/env bash
# Tags: no-fasttest

# Issue 130004: PuffinMetadata lists blobs of every type, Puffin still rejects blobs other than deletion-vector-v1.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
STATS="$CURDIR/data_minio/table_statistics_db"
COLUMNS="blob_type, snapshot_id, sequence_number, fields, offset, length, compression_codec, properties"

echo "--- Iceberg statistics file written by Spark"
$CLICKHOUSE_LOCAL -q "SELECT $COLUMNS FROM file('$STATS/ndv_table/metadata/3773046976092282465-10d5832c-06b4-4a5e-a13f-3f93274ee11f.stats', PuffinMetadata) ORDER BY offset"

echo "--- Iceberg statistics file written by Trino"
$CLICKHOUSE_LOCAL -q "SELECT $COLUMNS FROM file('$STATS/retained_table/metadata/20261104_210236_00005_f76mg-2bc7b3a4-2b9f-4222-8b40-f1ee36f46199.stats', PuffinMetadata) ORDER BY offset"

echo "--- deletion vector, theta sketch and an unknown blob type"
$CLICKHOUSE_LOCAL -q "SELECT $COLUMNS FROM file('$DATA/mixed_blob_types.puffin', PuffinMetadata) ORDER BY offset"

echo "--- Puffin rejects the same file"
for columns in referenced_data_file deleted_rows
do
    $CLICKHOUSE_LOCAL -q "SELECT $columns FROM file('$DATA/mixed_blob_types.puffin', Puffin)" 2>&1 \
        | grep -oF -e "Puffin blob 1: unsupported blob type 'apache-datasketches-theta-v1'" -e "/data/table/part-00000.parquet" -e "[2,5]"
done

echo "--- the generic blob metadata rules still apply"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/other_blob_codec_number.puffin', PuffinMetadata)" 2>&1 \
    | grep -oF "Puffin blob 0: field 'compression-codec' must be a string"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/other_blob_null_properties.puffin', PuffinMetadata)" 2>&1 \
    | grep -oF "Puffin blob 0: field 'properties' must be an object"
