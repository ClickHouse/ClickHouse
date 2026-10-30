#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and Iceberg (USE_AVRO)

# The query condition cache for `ORDER BY ... LIMIT n` (TopK) reads of Iceberg tables is keyed by the
# snapshot of the table, but the metadata of a data file (`_time`, `_etag`, `_tags`) can change without
# a new snapshot. A condition on it must not use the TopK entries: after the first data file stops
# passing the condition, the result comes from the row groups of the second one, which the entries of
# the first run recorded as holding no row of the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05397"
rm -rf "${ICEBERG_DIR}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05397"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05397 (k Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 1000"

${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --max_insert_threads 1 --query "
    INSERT INTO t_05397 SELECT 1000000 + number FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 --max_insert_threads 1 --query "
    INSERT INTO t_05397 SELECT number FROM numbers(10000)"

FIRST_FILE=$(${CLICKHOUSE_CLIENT} --query "SELECT _path FROM t_05397 WHERE k = 1000000")

# Both data files are older than the bound of the condition.
find "${ICEBERG_DIR}" -name '*.parquet' -exec touch -d '2020-01-01 00:00:00' {} +

QUERY="SELECT k FROM t_05397 WHERE _time < toDateTime('2021-01-01 00:00:00', 'UTC') ORDER BY k DESC LIMIT 3
    SETTINGS use_query_condition_cache = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "$QUERY"
# Only the modification time of the first data file changes, the snapshot stays the same.
touch -d '2026-01-01 00:00:00' "/${FIRST_FILE#/}"
${CLICKHOUSE_CLIENT} --query "$QUERY"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05397"
rm -rf "${ICEBERG_DIR}"
