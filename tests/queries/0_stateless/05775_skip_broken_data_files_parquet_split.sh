#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files
#
# A single Parquet file is normally split between several sources, which read its footer in advance.
# With `engine_file_skip_failed_data_files` the file is read by a single source instead, so that a broken
# file is skipped as a whole rather than failing the query while it is being split.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05775_skip_broken_parquet_split_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/good.parquet', Parquet)
    SELECT number AS id FROM numbers(100000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000
"

# A copy with a garbled footer.
cp "${DATA_DIR}/good.parquet" "${DATA_DIR}/broken.parquet"
SIZE=$(wc -c < "${DATA_DIR}/broken.parquet")
printf '%064d' 0 | dd of="${DATA_DIR}/broken.parquet" bs=1 seek=$((SIZE - 64)) conv=notrunc 2>/dev/null

SPLIT_SETTINGS="max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0"

echo "-- a healthy file is split between the sources"
"${LOCAL[@]}" --query "
    SELECT trimLeft(explain) FROM (
        EXPLAIN PIPELINE SELECT sum(id) FROM file('${DATA_DIR}/good.parquet') SETTINGS ${SPLIT_SETTINGS})
    WHERE trimLeft(explain) LIKE 'File %'"

echo "-- but not with the setting enabled"
"${LOCAL[@]}" --query "
    SELECT trimLeft(explain) FROM (
        EXPLAIN PIPELINE SELECT sum(id) FROM file('${DATA_DIR}/good.parquet')
        SETTINGS ${SPLIT_SETTINGS}, engine_file_skip_failed_data_files = 1)
    WHERE trimLeft(explain) LIKE 'File %'"

echo "-- a healthy file is read fully with the setting enabled"
"${LOCAL[@]}" --query "
    SELECT count(), sum(id) FROM file('${DATA_DIR}/good.parquet', Parquet, 'id UInt64')
    SETTINGS ${SPLIT_SETTINGS}, engine_file_skip_failed_data_files = 1"

echo "-- a broken file fails the query without the setting"
"${LOCAL[@]}" --query "
    SELECT sum(id) FROM file('${DATA_DIR}/broken.parquet', Parquet, 'id UInt64') SETTINGS ${SPLIT_SETTINGS}
" 2>&1 | grep -q "INCORRECT_DATA" && echo "1" || echo "0"

echo "-- and is skipped with the setting enabled"
"${LOCAL[@]}" --query "
    SELECT sum(id) FROM file('${DATA_DIR}/{good,broken}.parquet', Parquet, 'id UInt64')
    SETTINGS ${SPLIT_SETTINGS}, engine_file_skip_failed_data_files = 1" 2>/dev/null
"${LOCAL[@]}" --query "
    SELECT sum(id) FROM file('${DATA_DIR}/broken.parquet', Parquet, 'id UInt64')
    SETTINGS ${SPLIT_SETTINGS}, engine_file_skip_failed_data_files = 1" 2>/dev/null
