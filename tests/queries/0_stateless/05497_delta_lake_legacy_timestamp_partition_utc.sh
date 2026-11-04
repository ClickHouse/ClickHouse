#!/usr/bin/env bash
# Tags: no-fasttest

# The legacy Delta Lake reader (`allow_delta_kernel_rs = 0`) must accept timestamp
# partition values in the ISO8601 UTC form permitted by the Delta protocol, such as
# `2024-01-02T03:04:05.123456Z`, while still rejecting other trailing characters.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DELTA_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

make_table()
{
    local dir="$1"
    local ts="$2"
    rm -rf "$dir"
    mkdir -p "$dir/_delta_log" "$dir/part"
    $CLICKHOUSE_LOCAL -q "SELECT 1::Int64 AS id INTO OUTFILE '$dir/part/data.parquet' FORMAT Parquet"
    local size
    size=$(stat -c %s "$dir/part/data.parquet")
    cat > "$dir/_delta_log/00000000000000000000.json" <<JSON
{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE","operationParameters":{},"isBlindAppend":true}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"test-ts-partition","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["ts"],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"part/data.parquet","partitionValues":{"ts":"$ts"},"size":$size,"modificationTime":1700000000000,"dataChange":true}}
JSON
}

make_table "$DELTA_DIR/utc" "2024-01-02T03:04:05.123456Z"
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs 0 --session_timezone 'Asia/Tokyo' -q "SELECT id, toTimeZone(ts, 'UTC') FROM deltaLakeLocal('$DELTA_DIR/utc')"

make_table "$DELTA_DIR/plain" "2024-01-02 03:04:05.5"
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs 0 --session_timezone 'UTC' -q "SELECT id, ts FROM deltaLakeLocal('$DELTA_DIR/plain')"

make_table "$DELTA_DIR/bad" "2024-01-02 03:04:05abc"
$CLICKHOUSE_LOCAL --allow_delta_kernel_rs 0 -q "SELECT id, ts FROM deltaLakeLocal('$DELTA_DIR/bad')" 2>&1 | grep -oF -m1 "CANNOT_PARSE_INPUT_ASSERTION_FAILED"

rm -rf "$DELTA_DIR"
