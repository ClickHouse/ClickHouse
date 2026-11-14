#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: needs Parquet and Delta Lake
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `deltaLakeLocal` is absent.

# TopN dynamic filtering (`ORDER BY <column> LIMIT n`) on a Delta Lake table with column mapping, where a
# column is stored under the name of another column: the results with and without the optimization must
# agree. A column stored under its own name keeps the optimization.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_filter_push_down = 1,
    max_threads = 1, max_parsing_threads = 1, use_query_condition_cache = 0"
ON="use_top_k_dynamic_filtering = 1"
OFF="use_top_k_dynamic_filtering = 0"

DELTA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05620"
rm -rf "${DELTA_DIR}"
mkdir -p "${DELTA_DIR}/_delta_log"

# The data file stores `k` = number % 7, `v` = number and `w` = number: the table's `k_old`, `k` and `w`
# (column mapping enabled on an existing table, then `k` renamed to `k_old` and `v` to `k`).
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DELTA_DIR}/data.parquet', Parquet, 'k Int64, v Int64, w Int64')
    SELECT number % 7, number, number FROM numbers(100000) SETTINGS output_format_parquet_row_group_size = 10000"
DELTA_SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"k_old\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"k\"}},{\"name\":\"k\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"v\"}},{\"name\":\"w\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"w\"}}]}'
cat > "${DELTA_DIR}/_delta_log/00000000000000000000.json" <<LOG
{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}_05620","format":{"provider":"parquet","options":{}},"schemaString":"${DELTA_SCHEMA}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"3"},"createdTime":1700000000000}}
{"add":{"path":"data.parquet","partitionValues":{},"size":1,"modificationTime":1700000000000,"dataChange":true}}
LOG
DELTA="deltaLakeLocal('${DELTA_DIR}')"

echo "--- column stored under the name of another column"
${CLICKHOUSE_CLIENT} --query "SELECT k, k_old FROM ${DELTA} ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}, ${ON}"
${CLICKHOUSE_CLIENT} --query "SELECT k, k_old FROM ${DELTA} ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}, ${OFF}"

echo "--- column stored under its own name"
# Prints the result, and whether the query read fewer rows than the table has.
${CLICKHOUSE_CLIENT} --query "SELECT w FROM ${DELTA} ORDER BY w LIMIT 3 SETTINGS ${SETTINGS}, ${ON} FORMAT JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'read fewer rows:', d['statistics']['rows_read'] < 100000)"

rm -rf "${DELTA_DIR}"
