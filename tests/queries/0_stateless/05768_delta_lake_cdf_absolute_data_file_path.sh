#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The change data feed of a table which references its data files by absolute URI, as a shallow clone does:
# the paths are resolved like in a snapshot read, not joined to the directory of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="${CUR_DIR}/data_delta_lake/kernel_data/table-with-dv-small"
PARQUET="part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"

# A CDF-enabled table whose only commit adds the data file of `table-with-dv-small` at the URI $2.
write_log()
{
    mkdir -p "$1/_delta_log"
    cat > "$1/_delta_log/00000000000000000000.json" <<JSON
{"protocol":{"minReaderVersion":1,"minWriterVersion":4}}
{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}
{"add":{"path":"$2","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10}"}}
JSON
}

echo '-- file: URI'
write_log "$DIR/file" "file:$SRC/$PARQUET"
$CLICKHOUSE_LOCAL -q "SELECT sum(value), count(), groupUniqArray(_change_type), groupUniqArray(_commit_version)
    FROM deltaLakeLocal('$DIR/file') SETTINGS delta_lake_snapshot_start_version = 0"

echo '-- file:// URI'
write_log "$DIR/file_uri" "file://$SRC/$PARQUET"
$CLICKHOUSE_LOCAL -q "SELECT sum(value), count() FROM deltaLakeLocal('$DIR/file_uri') SETTINGS delta_lake_snapshot_start_version = 0"

echo '-- the snapshot of the same table'
$CLICKHOUSE_LOCAL -q "SELECT sum(value), count() FROM deltaLakeLocal('$DIR/file')"

rm -rf "$DIR"
