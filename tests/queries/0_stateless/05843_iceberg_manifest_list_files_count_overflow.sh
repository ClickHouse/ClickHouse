#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# The file counts of a manifest in an Iceberg manifest list are `int` in the specification, but their type is taken
# from the schema of the manifest list, so a malformed manifest list can have arbitrary `long` values in them.
# The sum of `added_files_count` and `existing_files_count` must be checked for overflow instead of being undefined behaviour.
# The manifest list in `data_iceberg/void_partition_malformed_manifest_list` is the manifest list of
# `void_partition_dropped_column` with these fields of the type `long` and the values 9223372036854775807 and 1 in the first entry.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="$CUR_DIR/data_iceberg/void_partition_dropped_column"
BAD="$CUR_DIR/data_iceberg/void_partition_malformed_manifest_list"
MANIFEST_LIST="snap-2011348865913312671-1-6a6f19d7-4d29-4fe1-9f6f-4cbaf3a20785.avro"

T="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_files_count_overflow"
rm -rf "$T"
cp -r "$SRC" "$T"
cp "$BAD/files_count_overflow.avro" "$T/metadata/$MANIFEST_LIST"
$CLICKHOUSE_LOCAL --query "SELECT id FROM icebergLocal('$T') ORDER BY id" 2>&1 | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
rm -rf "$T"
