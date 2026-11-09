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
# (`s3://a/b/1.avro`) that do not exist; describing the table needs only the metadata file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/spec/iceberg/table-metadata"
WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK"
trap 'rm -rf "$WORK"' EXIT

for name in TableMetadataV1Valid TableMetadataV2Valid TableMetadataV2ValidMinimal TableMetadataV3ValidMinimal \
    TableMetadataStatisticsFiles TableMetadataPartitionStatisticsFiles \
    TableMetadataUnsupportedVersion TableMetadataV1MissingSchemaType TableMetadataV2CurrentSchemaNotFound \
    TableMetadataV2MissingLastPartitionId TableMetadataV2MissingPartitionSpecs TableMetadataV2MissingSchemas \
    TableMetadataV2MissingSortOrder
do
    echo "--- $name"
    mkdir -p "$WORK/$name/metadata"
    cp "$DATA/$name.json" "$WORK/$name/metadata/v1.metadata.json"
    if ! $CLICKHOUSE_LOCAL --query "DESCRIBE icebergLocal('$WORK/$name') SETTINGS describe_compact_output = 1" 2>/dev/null
    then
        echo "exception"
    fi
done
