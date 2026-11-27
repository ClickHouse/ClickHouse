#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files, uses S3

# GROUP BY key LIMIT n over Parquet: the aggregation publishes the boundary of its top-K heap to the
# reader, which drops the rows beyond it and skips whole row groups by their statistics.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05816_parquet_group_by_top_k_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL}
    --enable_group_by_top_k_optimization=1
    --input_format_parquet_use_native_reader_v3=1
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --max_block_size=1000)
ON=(--enable_group_by_top_k_dynamic_filtering=1)
OFF=(--enable_group_by_top_k_dynamic_filtering=0)

# 20 row groups of 1000 rows with disjoint ranges of k, not in the order of k. Every value of k is in
# two row groups (i and i + 10), so a lost row of a group shows in its count. n has NULLs.
DATA="SELECT (number % 1000) + 1000 * ((intDiv(number, 1000) % 10 * 7 + 3) % 10) AS k, number % 3 AS g, number AS v,
    if(number % 4000 = 7, NULL, k) AS n
    FROM numbers(20000)"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
for i in 1 2 3; do
    "${LOCAL[@]}" --query "
        INSERT INTO FUNCTION file('${DIR}/m_${i}.parquet', Parquet) ${DATA} WHERE number % 3 = ${i} - 1
        SETTINGS output_format_parquet_row_group_size = 500, engine_file_truncate_on_insert = 1"
done

echo "-- the row groups beyond the boundary are skipped"
"${LOCAL[@]}" "${ON[@]}" --max_threads=1 --max_parsing_threads=1 --format JSON --query "
    SELECT k, count() FROM file('${DIR}/t.parquet') GROUP BY k ORDER BY k LIMIT 3" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print([list(r.values()) for r in d['data']], 'rows read <= 4000:', d['statistics']['rows_read'] <= 4000)"

echo "-- results identical with and without the optimization"
queries=(
    "SELECT k, count() FROM file('${DIR}/t.parquet') GROUP BY k ORDER BY k LIMIT 5"
    "SELECT k, count(), sum(v) FROM file('${DIR}/t.parquet') GROUP BY k ORDER BY k DESC LIMIT 5"
    "SELECT k, g, count() FROM file('${DIR}/t.parquet') GROUP BY k, g ORDER BY k, g LIMIT 7"
    "SELECT k, count() FROM file('${DIR}/t.parquet') WHERE v % 7 != 0 GROUP BY k ORDER BY k LIMIT 5"
    "SELECT n, count() FROM file('${DIR}/t.parquet') GROUP BY n ORDER BY n NULLS FIRST LIMIT 5"
    "SELECT n, count() FROM file('${DIR}/t.parquet') GROUP BY n ORDER BY n DESC NULLS LAST LIMIT 5"
    "SELECT k, count() FROM file('${DIR}/m_*.parquet') GROUP BY k ORDER BY k LIMIT 5"
    "SELECT k, count() FROM file('${DIR}/m_*.parquet') GROUP BY k ORDER BY k DESC LIMIT 5"
    "SELECT * FROM (SELECT k, count() AS c FROM file('${DIR}/t.parquet') GROUP BY k LIMIT 5) ORDER BY k"
)
for query in "${queries[@]}"; do
    for threads in 1 8; do
        diff \
            <("${LOCAL[@]}" --max_threads=${threads} --max_parsing_threads=${threads} "${ON[@]}" --query "${query}") \
            <("${LOCAL[@]}" --max_threads=${threads} --max_parsing_threads=${threads} "${OFF[@]}" --query "${query}") \
            && echo "OK"
    done
done

echo "-- s3"
S3_PATH="${CLICKHOUSE_DATABASE}/05816_group_by_top_k.parquet"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}', format = Parquet) ${DATA}
    SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"
S3_QUERY="SELECT k, count(), sum(v) FROM s3(s3_conn, filename = '${S3_PATH}', format = Parquet) GROUP BY k ORDER BY k LIMIT 5"
diff \
    <(${CLICKHOUSE_CLIENT} --enable_group_by_top_k_dynamic_filtering=1 --query "${S3_QUERY}") \
    <(${CLICKHOUSE_CLIENT} --enable_group_by_top_k_dynamic_filtering=0 --query "${S3_QUERY}") \
    && echo "OK"
