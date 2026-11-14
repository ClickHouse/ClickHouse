#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The table metadata files of the Apache Iceberg Java implementation (`core/src/test/resources/TableMetadata*.json`),
# see `data_iceberg/spec/README.md`. Each file becomes `metadata/v1.metadata.json` of a table.
# The valid files must be described with the columns of the current schema, the invalid ones must be rejected,
# as `TestTableMetadata` (Java) and `metadata_serde_test.cc` (iceberg-cpp, the same files) expect:
# - unsupported format version 42 (`Cannot read unsupported version`);
# - format version 1 schema without `"type": "struct"` (`Missing 'type'`);
# - format version 2 without `partition-specs`, `last-partition-id`, `sort-orders` or `schemas`,
#   or with `current-schema-id` not in `schemas`.
# The snapshots of `TableMetadataV2Valid.json` and `TableMetadata*StatisticsFiles.json` refer to manifest lists
# `s3://a/b/1.avro` and `s3://a/b/2.avro`, which are not supplied and are outside the table location
# `s3://bucket/test/location`. Describing the table reads the manifest list of the current snapshot,
# so the test remaps them into `metadata/` of the table and supplies empty manifest lists.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/spec/iceberg/table-metadata"
WORK="$(realpath -m "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}")"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'rm -rf "$WORK"' EXIT

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`.
cd "$WORK" || exit 1

# An empty manifest list: an Avro file with the fields of the `manifest_file` struct of the specification and no rows.
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM values('manifest_path String, manifest_length Int64, partition_spec_id Int32, content Int32,
        sequence_number Int64, min_sequence_number Int64, added_snapshot_id Int64,
        added_files_count Int32, existing_files_count Int32, deleted_files_count Int32,
        added_rows_count Int64, existing_rows_count Int64, deleted_rows_count Int64',
        ('', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    WHERE 0 FORMAT Avro" > empty-manifest-list.avro

for name in TableMetadataV1Valid TableMetadataV2Valid TableMetadataV2ValidMinimal TableMetadataV3ValidMinimal \
    TableMetadataStatisticsFiles TableMetadataPartitionStatisticsFiles \
    TableMetadataUnsupportedVersion TableMetadataV1MissingSchemaType TableMetadataV2CurrentSchemaNotFound \
    TableMetadataV2MissingLastPartitionId TableMetadataV2MissingPartitionSpecs TableMetadataV2MissingSchemas \
    TableMetadataV2MissingSortOrder
do
    echo "--- $name"
    mkdir -p "$name/metadata"
    sed 's|"s3://a/b/|"s3://bucket/test/location/metadata/|g' "$DATA/$name.json" > "$name/metadata/v1.metadata.json"
    cp empty-manifest-list.avro "$name/metadata/1.avro"
    cp empty-manifest-list.avro "$name/metadata/2.avro"
    if ! $CLICKHOUSE_LOCAL --query "DESCRIBE icebergLocal('$name') SETTINGS describe_compact_output = 1" 2>/dev/null
    then
        echo "exception"
    fi
done
