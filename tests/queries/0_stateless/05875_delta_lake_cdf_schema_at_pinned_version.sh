#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The change data feed without an end version ends at the version pinned by `delta_lake_snapshot_version`,
# also when the schema is inferred: a later commit which changes the schema of the table must not be opened.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="${CUR_DIR}/data_delta_lake/kernel_data/table-with-dv-small"
PARQUET="part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "$DIR"
mkdir -p "$DIR/_delta_log"

# Version 0 adds a data file with the column `value`, version 1 adds the column `extra` to the schema.
cat > "$DIR/_delta_log/00000000000000000000.json" <<JSON
{"protocol":{"minReaderVersion":1,"minWriterVersion":4}}
{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}
{"add":{"path":"file:$SRC/$PARQUET","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10}"}}
JSON
cat > "$DIR/_delta_log/00000000000000000001.json" <<JSON
{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"extra\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}
JSON

for reload in 0 1; do
    echo "-- delta_lake_reload_schema_for_consistency = $reload"
    $CLICKHOUSE_LOCAL -q "DESCRIBE TABLE deltaLakeLocal('$DIR')
    SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_snapshot_version = 0, delta_lake_reload_schema_for_consistency = $reload" | cut -f1

    $CLICKHOUSE_LOCAL -q "
    SELECT sum(value), count(), groupUniqArray(_change_type), groupUniqArray(_commit_version) FROM deltaLakeLocal('$DIR')
    SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_snapshot_version = 0, delta_lake_reload_schema_for_consistency = $reload"
done

echo '-- the same range with an explicit end version'
$CLICKHOUSE_LOCAL -q "SELECT sum(value), count() FROM deltaLakeLocal('$DIR')
    SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_snapshot_end_version = 0"

rm -rf "$DIR"
