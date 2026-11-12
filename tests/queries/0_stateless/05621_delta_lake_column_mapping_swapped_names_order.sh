#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: needs Parquet and Delta Lake
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `deltaLakeLocal` is absent.

# A Delta Lake table with column mapping where a column is stored under the logical name of another column, read
# with the columns in every order: the renaming of the requested columns to their physical names must not collide
# with a logical name that is not renamed yet (it used to erase the column and read freed memory).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DELTA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05621"
rm -rf "${DELTA_DIR}"
mkdir -p "${DELTA_DIR}/_delta_log"

# The data file stores `k` = number % 7, `v` = number and `w` = number: the table's `k_old`, `k` and `w`
# (column mapping enabled on an existing table, then `k` renamed to `k_old` and `v` to `k`).
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DELTA_DIR}/data.parquet', Parquet, 'k Int64, v Int64, w Int64')
    SELECT number % 7, number, number FROM numbers(100000)"
DELTA_SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"k_old\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"k\"}},{\"name\":\"k\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"v\"}},{\"name\":\"w\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"w\"}}]}'
cat > "${DELTA_DIR}/_delta_log/00000000000000000000.json" <<LOG
{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}_05621","format":{"provider":"parquet","options":{}},"schemaString":"${DELTA_SCHEMA}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"3"},"createdTime":1700000000000}}
{"add":{"path":"data.parquet","partitionValues":{},"size":1,"modificationTime":1700000000000,"dataChange":true}}
LOG
DELTA="deltaLakeLocal('${DELTA_DIR}')"

${CLICKHOUSE_CLIENT} --query "SELECT k_old, k FROM ${DELTA} ORDER BY k DESC LIMIT 3"
${CLICKHOUSE_CLIENT} --query "SELECT k, k_old FROM ${DELTA} ORDER BY k DESC LIMIT 3"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DELTA} ORDER BY w LIMIT 3"
${CLICKHOUSE_CLIENT} --query "SELECT sum(k_old), sum(k), sum(w) FROM ${DELTA}"
${CLICKHOUSE_CLIENT} --query "SELECT k_old FROM ${DELTA} WHERE k = 12345"

rm -rf "${DELTA_DIR}"
