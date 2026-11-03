#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-parallel-replicas: counts the S3 requests of the initial query, which with parallel replicas resolves the table again for the query it sends to the replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="05407_iceberg_metadata_s3_requests/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"

run_query()
{
    ${CLICKHOUSE_CLIENT} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" \
        --query "$2 SETTINGS use_iceberg_metadata_files_cache = 1, iceberg_metadata_staleness_ms = 0" > /dev/null
}

run_query table_function_select "SELECT * FROM icebergS3(s3_conn, filename = '${TABLE_PATH}')"
run_query table_function_count "SELECT count() FROM icebergS3(s3_conn, filename = '${TABLE_PATH}')"
run_query table_select "SELECT * FROM ${TABLE}"
run_query table_count "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''), ProfileEvents['S3ListObjects'], ProfileEvents['S3GetObject']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'
    ORDER BY event_time_microseconds
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
