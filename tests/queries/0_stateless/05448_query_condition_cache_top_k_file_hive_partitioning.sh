#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# An `ORDER BY ... LIMIT` read through the `file` table function must not reuse the query condition cache
# entries of the same query with another `use_hive_partitioning`: `key` comes from the path in one and
# from the file in the other.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05448"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/key=1/data.parquet', Parquet, 'k UInt64, key UInt64')
    SELECT number, number < 1000 FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, use_hive_partitioning = 0"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/key=1/data.parquet"

SETTINGS="use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
    query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_use_native_reader_v3 = 1, max_threads = 1"

for hive in 1 0 1 0
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT groupArray(k) FROM (SELECT k FROM file('${DATA_DIR}/key=1/data.parquet') WHERE key = 1 ORDER BY k DESC LIMIT 3)
        SETTINGS ${SETTINGS}, use_hive_partitioning = ${hive}"
done

rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
