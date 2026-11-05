#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and Iceberg (USE_AVRO)

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of Iceberg tables is keyed by the
# snapshot of the table, but `RENAME TABLE` changes `_table` without a new snapshot and keeps the UUID
# (the storage reports the new name after it is attached again). A condition on `_table` must not use
# the TopK entries: after the rename the rows of the first data file stop passing the condition, and the
# result comes from the row groups of the second one, which the entries of the first run recorded as
# holding no row of the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05406"
rm -rf "${ICEBERG_DIR}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05406_before"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05406_after"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05406_before (k Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 1000"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --max_insert_threads 1 --query "
    INSERT INTO t_05406_before SELECT 1000000 + number FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --max_insert_threads 1 --query "
    INSERT INTO t_05406_before SELECT number FROM numbers(10000)"

SETTINGS="SETTINGS use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "SELECT k FROM t_05406_before WHERE _table = 't_05406_before' OR k < 1000000 ORDER BY k DESC LIMIT 3 $SETTINGS"
${CLICKHOUSE_CLIENT} --query "RENAME TABLE t_05406_before TO t_05406_after"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_05406_after"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_05406_after"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM t_05406_after WHERE _table = 't_05406_before' OR k < 1000000 ORDER BY k DESC LIMIT 3 $SETTINGS"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05406_after"
rm -rf "${ICEBERG_DIR}"
