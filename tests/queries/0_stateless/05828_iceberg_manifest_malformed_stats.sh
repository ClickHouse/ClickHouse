#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The statistics of a data file in an Iceberg manifest are Avro maps, read as arrays of key-value tuples. The tuple
# type is inferred from the schema of the manifest, so a malformed manifest can make it have fewer than two elements.
# Such a manifest must be rejected with an exception instead of being indexed out of bounds.
# The manifests in `data_iceberg/void_partition_malformed_stats` are a manifest of `void_partition_dropped_column`
# with the items of `value_counts` of an empty record and the items of `lower_bounds` of a record with only `key`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="$CUR_DIR/data_iceberg/void_partition_dropped_column"
BAD="$CUR_DIR/data_iceberg/void_partition_malformed_stats"
MANIFEST="2c1a544a-8aca-4216-bcdb-683d33f1e605.avro"

for bad in value_counts_empty_record lower_bounds_one_field_record
do
    T="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${bad}"
    rm -rf "$T"
    cp -r "$SRC" "$T"
    cp "$BAD/$bad.avro" "$T/metadata/$MANIFEST"
    $CLICKHOUSE_LOCAL --query "SELECT id FROM icebergLocal('$T') ORDER BY id" 2>&1 | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
    rm -rf "$T"
done
