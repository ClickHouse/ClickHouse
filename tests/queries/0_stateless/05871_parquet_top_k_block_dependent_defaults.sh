#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files, uses S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05871_parquet_top_k_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

# 20 row groups of 1000 rows. The first two hold k = 0..9, the others k >= 10: the boundary k = 0 is
# set by the first row group, drops rows of the second one and skips the rest by their statistics.
DATA="SELECT intDiv(number, 2000) * 10 + number % 10 AS k, number AS v FROM numbers(20000)"
SETTINGS="max_threads = 1, max_parsing_threads = 1, max_block_size = 1000, use_query_condition_cache = 0, max_rows_to_group_by = 0,
    input_format_parquet_preserve_order = 1,
    enable_group_by_top_k_optimization = 1, group_by_top_k_optimization_observation_rows = 0"
ON="enable_group_by_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering = 1"
OFF="enable_group_by_top_k_dynamic_filtering = 0, use_top_k_dynamic_filtering = 0"

schemas=(
    "k UInt64, v UInt64, g UInt64 DEFAULT v % 7"
    "k UInt64, v UInt64, g UInt64 DEFAULT rowNumberInBlock()"
    "k UInt64, v UInt64, g UInt64 DEFAULT blockSize()"
    "k UInt64, v UInt64 DEFAULT 1000000000, g UInt64 DEFAULT if(v < 1000000000, rowNumberInBlock(), 42)"
)

queries()
{
    echo "SELECT k, count(), sum(v), sum(g) FROM t GROUP BY k ORDER BY k LIMIT 1 SETTINGS ${SETTINGS}, $1;
        SELECT k, g, max(v) FROM t GROUP BY k, g ORDER BY k, g LIMIT 3 SETTINGS ${SETTINGS}, $1;"
}

ROWS_READ_QUERY="SELECT k, count(), sum(v), sum(g) FROM t GROUP BY k ORDER BY k LIMIT 1 SETTINGS ${SETTINGS}, ${ON}"

skipped()
{
    python3 -c "import sys, json; print('row groups skipped:', int(json.load(sys.stdin)['statistics']['rows_read'] < 20000))"
}

echo "-- file"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
for schema in "${schemas[@]}"; do
    echo "${schema}"
    CREATE="CREATE TABLE t (${schema}) ENGINE = File(Parquet, '${DIR}/t.parquet');"
    ${CLICKHOUSE_LOCAL} --format JSON --query "${CREATE} ${ROWS_READ_QUERY}" | skipped
    diff \
        <(${CLICKHOUSE_LOCAL} --query "${CREATE} $(queries "${ON}")") \
        <(${CLICKHOUSE_LOCAL} --query "${CREATE} $(queries "${OFF}")") \
        && echo "OK"
done

echo "-- s3"
S3_PATH="${CLICKHOUSE_DATABASE}/05871_top_k_block_dependent_defaults.parquet"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}', format = Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"
for schema in "${schemas[@]}"; do
    echo "${schema}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t; CREATE TABLE t (${schema}) ENGINE = S3(s3_conn, filename = '${S3_PATH}', format = Parquet)"
    ${CLICKHOUSE_CLIENT} --format JSON --query "${ROWS_READ_QUERY}" | skipped
    diff \
        <(${CLICKHOUSE_CLIENT} --query "$(queries "${ON}")") \
        <(${CLICKHOUSE_CLIENT} --query "$(queries "${OFF}")") \
        && echo "OK"
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
