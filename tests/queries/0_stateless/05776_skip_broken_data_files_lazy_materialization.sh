#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files
#
# Lazy materialization reads the columns that are not needed for sorting in a second pass over the
# rows recorded by the main pass. A file that turns out to be broken only in the second pass could not
# be skipped without breaking the alignment of the two passes, so `engine_file_skip_failed_data_files`
# disables the optimization.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05776_skip_broken_lazy_mat_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet', Parquet)
    SELECT number AS k, concat('val_', toString(number)) AS s FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100
"

QUERY="SELECT s FROM file('${DATA_DIR}/data.parquet', Parquet) ORDER BY k LIMIT 3"

echo "-- the number of lazy read steps in the plan"
"${LOCAL[@]}" --query "
    SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN ${QUERY})
    SETTINGS query_plan_optimize_lazy_materialization_for_file = 1"

echo "-- with the setting enabled"
"${LOCAL[@]}" --query "
    SELECT countIf(explain LIKE '%LazilyReadFromFile%') FROM (EXPLAIN ${QUERY})
    SETTINGS query_plan_optimize_lazy_materialization_for_file = 1, engine_file_skip_failed_data_files = 1"

echo "-- the result is the same"
"${LOCAL[@]}" --query "${QUERY} SETTINGS query_plan_optimize_lazy_materialization_for_file = 1, engine_file_skip_failed_data_files = 1"
