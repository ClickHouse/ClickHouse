#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reads Parquet files from MinIO.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FILE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
S3_NAME="${CLICKHOUSE_DATABASE}_scan_read_limits.parquet"
trap 'rm -f "${CLICKHOUSE_USER_FILES}/${FILE_NAME}"' EXIT
rm -f "${CLICKHOUSE_USER_FILES}/${FILE_NAME}"

FILE_SOURCE="file('${FILE_NAME}', Parquet, 'n UInt64, payload String')"
S3_SOURCE="s3(s3_conn, filename = '${S3_NAME}', format = Parquet, structure = 'n UInt64, payload String')"
URL_SOURCE="url('http://localhost:11111/test/${S3_NAME}', Parquet, 'n UInt64, payload String')"

# The decoded payload is over 6 MB, while the ZSTD-compressed file is only a few KB.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION ${FILE_SOURCE}
    SELECT number AS n, concat(toString(number), repeat('x', 65536)) AS payload FROM numbers(100)
    SETTINGS output_format_parquet_compression_method = 'zstd', output_format_parquet_row_group_size = 10;
    INSERT INTO FUNCTION ${S3_SOURCE} SELECT * FROM ${FILE_SOURCE}
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_compression_method = 'zstd', output_format_parquet_row_group_size = 10;
"

CLIENT=(${CLICKHOUSE_CLIENT}
    --enable_analyzer=1 --make_distributed_plan=0
    --max_threads=1 --max_parsing_threads=1 --max_block_size=1
    --input_format_parquet_use_native_reader_v3=1 --input_format_parquet_max_block_size=1
    --input_format_parquet_filter_push_down=0 --input_format_parquet_page_filter_push_down=0
    --input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0
    --query_plan_optimize_lazy_materialization=0 --optimize_read_in_order=0 --use_query_condition_cache=0
    --read_overflow_mode=throw --read_overflow_mode_leaf=throw
    --use_cache_for_count_from_files=1 --log_queries=1 --log_profile_events=1 --log_queries_min_query_duration_ms=0)

source_index=0
for source in "${FILE_SOURCE}" "${S3_SOURCE}" "${URL_SOURCE}"; do
    query_prefix="${CLICKHOUSE_TEST_UNIQUE_NAME}_${source_index}"
    source_index=$((source_index + 1))
    rejected="SELECT sum(n) FROM ${source} PREWHERE modulo(n, 1000000) = 999999"
    payload="SELECT sum(length(payload)) FROM ${source}"

    "${CLIENT[@]}" --query_id="${query_prefix}_progress" --query "SELECT sum(n) FROM ${source}"
    "${CLIENT[@]}" --query "${rejected}"
    "${CLIENT[@]}" --query "${payload}"

    # Rows rejected by `PREWHERE` must still consume the total and leaf scan budgets.
    for limit in max_rows_to_read max_rows_to_read_leaf; do
        "${CLIENT[@]}" --query "${rejected} SETTINGS ${limit} = 10" 2>&1 | grep -m1 -o 'TOO_MANY_ROWS'
    done
    for limit in max_bytes_to_read max_bytes_to_read_leaf; do
        "${CLIENT[@]}" --query "${rejected} SETTINGS ${limit} = 10" 2>&1 | grep -m1 -o 'TOO_MANY_BYTES'
        "${CLIENT[@]}" --query "${payload} SETTINGS ${limit} = 1000000" 2>&1 | grep -m1 -o 'TOO_MANY_BYTES'
    done

    # One-row blocks make partial reads deterministic. Both columns must stay aligned.
    for suffix in '' _leaf; do
        "${CLIENT[@]}" --query "SELECT count() > 0 AND count() < 100
            AND countIf(payload != concat(toString(n), repeat('x', 65536))) = 0
            FROM ${source} SETTINGS max_bytes_to_read${suffix} = 1000000, read_overflow_mode${suffix} = 'break'"
        # A stopped scan must not replace the file's row-count cache with a partial count.
        "${CLIENT[@]}" --query "SELECT count() FROM ${source}"
        "${CLIENT[@]}" --query_id="${query_prefix}_break${suffix}" --query "${rejected}
            SETTINGS max_rows_to_read${suffix} = 10, read_overflow_mode${suffix} = 'break'"
    done

    # An unread row group must not be cached as failing the predicate after a partial scan.
    "${CLIENT[@]}" --query "SELECT sum(n) FROM ${source} PREWHERE modulo(n, 1000000) = 99
        SETTINGS use_query_condition_cache = 1, max_rows_to_read = 10, read_overflow_mode = 'break' FORMAT Null"
    "${CLIENT[@]}" --query "SELECT sum(n) FROM ${source} PREWHERE modulo(n, 1000000) = 99
        SETTINGS use_query_condition_cache = 1"

    # Reuse the `PREWHERE` column in the output; neither its rows nor its bytes may be counted twice.
    "${CLIENT[@]}" --query_id="${query_prefix}_prewhere" --query "SELECT sum(n) FROM ${source} PREWHERE modulo(n, 2) = 0
        SETTINGS max_rows_to_read = 100, max_rows_to_read_leaf = 100, max_bytes_to_read = 800, max_bytes_to_read_leaf = 800"
    "${CLIENT[@]}" --query "SYSTEM FLUSH LOGS"
    "${CLIENT[@]}" --query "SELECT count() = 2 AND countIf(read_rows = 100 AND read_bytes = 800) = 2
        FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ('${query_prefix}_progress', '${query_prefix}_prewhere')"
    "${CLIENT[@]}" --query "SELECT count() = 2 AND countIf(ProfileEvents['ParquetRowsFilterExpression'] < 100) = 2
        FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ('${query_prefix}_break', '${query_prefix}_break_leaf')"

    # Output-format statistics must receive the same scan progress as limits and `query_log`.
    ${CLICKHOUSE_CURL} -fsS "${CLICKHOUSE_URL}" --data-binary "SELECT sum(n) FROM ${source}
        SETTINGS enable_analyzer = 1, make_distributed_plan = 0, max_threads = 1, max_parsing_threads = 1,
            max_rows_to_read = 100, max_bytes_to_read = 800
        FORMAT JSON" | python3 -c 'import json, sys; s = json.load(sys.stdin)["statistics"]; print(int(s["rows_read"] == 100 and s["bytes_read"] == 800))'
done

# The same compressed payload must consume the byte budget when it is read in a deferred pass.
for source in "${FILE_SOURCE}" "${S3_SOURCE}"; do
    lazy_query="SELECT sum(length(payload)) FROM (SELECT payload FROM ${source} ORDER BY n LIMIT 20)"
    for limit in max_bytes_to_read max_bytes_to_read_leaf; do
        "${CLIENT[@]}" --query "${lazy_query} SETTINGS query_plan_optimize_lazy_materialization = 1,
            enable_analyzer = 1, make_distributed_plan = 0, query_plan_max_limit_for_lazy_materialization = 0,
            query_plan_optimize_lazy_materialization_for_file = 1,
            query_plan_optimize_lazy_materialization_for_object_storage = 1, s3_validate_etag_on_read = 1,
            ${limit} = 1000000" 2>&1 | grep -m1 -o 'TOO_MANY_BYTES'
    done
done
