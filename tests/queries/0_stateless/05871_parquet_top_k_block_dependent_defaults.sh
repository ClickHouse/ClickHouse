#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files, uses S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05871_parquet_top_k_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

DATA="SELECT (number + 500) % 1000 AS k, number AS v FROM numbers(2000000)"
COLUMNS="k UInt64, v UInt64, g UInt64 DEFAULT rowNumberInBlock(), b UInt64 DEFAULT blockSize()"
SETTINGS="max_threads = 1, max_parsing_threads = 1, max_block_size = 1000, use_query_condition_cache = 0,
    input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_preserve_order = 1,
    enable_group_by_top_k_optimization = 1, group_by_top_k_optimization_observation_rows = 0"
ON="enable_group_by_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering = 1"
OFF="enable_group_by_top_k_dynamic_filtering = 0, use_top_k_dynamic_filtering = 0"

queries=(
    "SELECT k, count(), sum(g) FROM t GROUP BY k ORDER BY k LIMIT 1"
    "SELECT k, count(), sum(b) FROM t GROUP BY k ORDER BY k LIMIT 1"
    "SELECT k, g FROM t GROUP BY k, g ORDER BY k, g LIMIT 3"
    "SELECT k, v, g FROM t ORDER BY k, v LIMIT 3"
)

echo "-- file"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
for query in "${queries[@]}"; do
    diff \
        <(${CLICKHOUSE_LOCAL} --query "CREATE TABLE t (${COLUMNS}) ENGINE = File(Parquet, '${DIR}/t.parquet'); ${query} SETTINGS ${SETTINGS}, ${ON}") \
        <(${CLICKHOUSE_LOCAL} --query "CREATE TABLE t (${COLUMNS}) ENGINE = File(Parquet, '${DIR}/t.parquet'); ${query} SETTINGS ${SETTINGS}, ${OFF}") \
        && echo "OK"
done

echo "-- s3"
S3_PATH="${CLICKHOUSE_DATABASE}/05871_top_k_block_dependent_defaults.parquet"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}', format = Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (${COLUMNS}) ENGINE = S3(s3_conn, filename = '${S3_PATH}', format = Parquet)"
for query in "${queries[@]}"; do
    diff \
        <(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS ${SETTINGS}, ${ON}") \
        <(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS ${SETTINGS}, ${OFF}") \
        && echo "OK"
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
