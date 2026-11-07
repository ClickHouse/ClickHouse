#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Iceberg and Parquet
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache, which a parallel test can clear

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05428_iceberg_qcc/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_iceberg_qcc"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_iceberg_qcc (k Int64, s String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS output_format_parquet_row_group_size = 1000"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    INSERT INTO t_iceberg_qcc SELECT number, toString(number) FROM numbers(3000)"

for run in 1 2; do
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_05428_${run}" --use_query_condition_cache 1 --query "
        SELECT count(), sum(k) FROM icebergLocal('${TABLE_PATH}') WHERE k BETWEEN 1100 AND 1200"
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheMisses'] > 0, ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '${CLICKHOUSE_DATABASE}_05428_%'
    ORDER BY query_id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_iceberg_qcc"
rm -rf "${TABLE_PATH}"
