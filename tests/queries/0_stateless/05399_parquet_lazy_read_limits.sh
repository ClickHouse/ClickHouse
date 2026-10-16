#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reads Parquet files from MinIO.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FILE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
trap 'rm -f "${CLICKHOUSE_USER_FILES}/${FILE_NAME}"' EXIT
rm -f "${CLICKHOUSE_USER_FILES}/${FILE_NAME}"

FILE_SOURCE="file('${FILE_NAME}', Parquet, 'k UInt64, payload String')"
S3_SOURCE="s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}_lazy_read_limits.parquet', format = Parquet, structure = 'k UInt64, payload String')"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION ${FILE_SOURCE}
    SELECT number AS k, randomString(65536) AS payload FROM numbers(100)
    SETTINGS output_format_parquet_compression_method = 'none';
    INSERT INTO FUNCTION ${S3_SOURCE} SELECT * FROM ${FILE_SOURCE}
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_compression_method = 'none';
"

CLIENT=(${CLICKHOUSE_CLIENT}
    --enable_analyzer=1 --make_distributed_plan=0
    --query_plan_optimize_lazy_materialization=1 --query_plan_max_limit_for_lazy_materialization=0
    --query_plan_optimize_lazy_materialization_for_file=1
    --query_plan_optimize_lazy_materialization_for_object_storage=1 --s3_validate_etag_on_read=1
    --max_threads=1 --max_block_size=1 --input_format_parquet_max_block_size=1)

for source in "${FILE_SOURCE}" "${S3_SOURCE}"; do
    QUERY="SELECT sum(length(payload)) FROM (SELECT payload FROM ${source} ORDER BY k LIMIT 20)"

    # Prove that the payload is deferred, and that the key scan alone fits both byte budgets.
    "${CLIENT[@]}" --query "SELECT countIf(explain LIKE '%LazilyReadFrom%') FROM (EXPLAIN ${QUERY})"
    "${CLIENT[@]}" --query "SELECT sum(k) FROM (SELECT k FROM ${source} ORDER BY k LIMIT 20)
        SETTINGS max_bytes_to_read = 1000000, max_bytes_to_read_leaf = 1000000"
    "${CLIENT[@]}" --query "${QUERY}"

    # The deferred payload crosses the budget after the main source has already finished.
    for limit in max_bytes_to_read max_bytes_to_read_leaf; do
        "${CLIENT[@]}" --query "${QUERY} SETTINGS ${limit} = 1000000" 2>&1 | grep -m1 -o 'TOO_MANY_BYTES'
    done

    # One-row Parquet blocks ensure that the deferred pass can stop before the full result.
    "${CLIENT[@]}" --query "SELECT (${QUERY}) < 1310720 SETTINGS max_bytes_to_read = 1000000, read_overflow_mode = 'break'"
    "${CLIENT[@]}" --query "SELECT (${QUERY}) < 1310720 SETTINGS max_bytes_to_read_leaf = 1000000, read_overflow_mode_leaf = 'break'"
done
