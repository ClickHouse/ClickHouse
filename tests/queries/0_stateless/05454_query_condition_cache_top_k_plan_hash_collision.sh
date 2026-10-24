#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# The query condition cache keys the entries of `ORDER BY ... LIMIT n` (TopK) reads by the TopK plan, which
# includes the sort column and its type. `c UInt8` and `cU Int8` must give different plans, even though their
# names and types run into the same string. The entries of `ORDER BY cU DESC LIMIT 3` record the row groups /
# granules that hold no row of its result, among them the one with the largest values of `c`, so
# `ORDER BY c DESC LIMIT 3` must not use them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
    query_plan_max_limit_for_top_k_optimization = 1000"

function run()
{
    local table=$1
    local where=$2
    local extra_settings=$3
    for column in cU c
    do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT groupArray(${column}) FROM (SELECT ${column} FROM ${table} ${where} ORDER BY ${column} DESC LIMIT 3)
            SETTINGS ${SETTINGS}, ${extra_settings}"
    done
}

echo "--- File"
# `cU` has its largest values only in the first row group, `c` only in the last one.
DATA_DIR="${CLICKHOUSE_DATABASE}_05454"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet')
    SELECT toUInt8(if(number >= 99000, 200, 1)) AS c, toInt8(if(number < 1000, 100, 0)) AS cU FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/data.parquet"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05454_file (c UInt8, cU Int8) ENGINE = File(Parquet, '${DATA_DIR}/data.parquet')"
run t_05454_file "" "input_format_parquet_use_native_reader_v3 = 1"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05454_file"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"

echo "--- MergeTree"
# The `WHERE` stays a separate filter, whose entries are keyed by the condition and the TopK plan. In the last
# granule, the rows that pass the TopK filter of `cU` fail the `WHERE`, and the rows with the largest values of
# `c` pass it. One thread and small blocks, so the threshold is known before the last granule is read.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05454_mt (c UInt8, cU Int8, x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t_05454_mt SELECT
        toUInt8(if(number >= 99000 AND number % 2 = 1, 200, 1)) AS c,
        toInt8(if(number < 1000 OR (number >= 99000 AND number % 2 = 0), 100, 0)) AS cU,
        toUInt8(number >= 99000 AND number % 2 = 0) AS x
    FROM numbers(100000)"
run t_05454_mt "WHERE x = 0" "optimize_move_to_prewhere = 0, max_threads = 1, max_block_size = 1000"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05454_mt"
