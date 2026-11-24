#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files, uses S3

# A grouping key computed by `joinGet` decides the boundary of the GROUP BY top-K heap, but the contents of
# the `Join` table are not part of the query condition cache key. A rerun after they change must not reuse
# the row groups (granules) the first run excluded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05872"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
trap 'rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"' EXIT

S3_PATH="${CLICKHOUSE_DATABASE}/05872"
for k in 1 2; do
    DATA="SELECT ${k} AS k, number AS v FROM numbers(1000)"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DATA_DIR}/p_${k}.parquet') ${DATA}
        SETTINGS output_format_parquet_row_group_size = 100, engine_file_truncate_on_insert = 1"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}/p_${k}.parquet', format = Parquet) ${DATA}
        SETTINGS output_format_parquet_row_group_size = 100, s3_truncate_on_insert = 1"
done
# Backdate the files so their version tokens have settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}"/p_*.parquet

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_file;
    DROP TABLE IF EXISTS t_s3;
    DROP TABLE IF EXISTS t_merge_tree;
    DROP TABLE IF EXISTS lookup;
    CREATE TABLE t_file (k UInt64, v UInt64) ENGINE = File(Parquet, '${DATA_DIR}/p_*.parquet');
    CREATE TABLE t_s3 (k UInt64, v UInt64) ENGINE = S3(s3_conn, filename = '${S3_PATH}/p_*.parquet', format = Parquet);
    CREATE TABLE t_merge_tree (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100;
    SYSTEM STOP MERGES t_merge_tree;
    INSERT INTO t_merge_tree SELECT 1, number FROM numbers(1000);
    INSERT INTO t_merge_tree SELECT 2, number FROM numbers(1000);
    CREATE TABLE lookup (v UInt64, val UInt64) ENGINE = Join(ANY, LEFT, v);"

SETTINGS="max_threads = 1, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1,
    input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_preserve_order = 1,
    enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1,
    group_by_top_k_optimization_observation_rows = 0"

for table in t_file t_s3 t_merge_tree; do
    echo "-- ${table}"
    QUERY="SELECT k, joinGet('lookup', 'val', v) AS g, count() FROM ${table} GROUP BY k, g ORDER BY k, g LIMIT 2 SETTINGS ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE lookup; INSERT INTO lookup SELECT number, number FROM numbers(1000)"
    ${CLICKHOUSE_CLIENT} --query "${QUERY}"
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE lookup; INSERT INTO lookup SELECT number, 0 FROM numbers(1000)"
    ${CLICKHOUSE_CLIENT} --query "${QUERY}"
done

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE t_file;
    DROP TABLE t_s3;
    DROP TABLE t_merge_tree;
    DROP TABLE lookup;"
