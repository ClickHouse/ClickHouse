#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs Iceberg (USE_AVRO) and S3

# A parallel INSERT creates `max_insert_threads` Iceberg sinks, and a sink that receives no data commits nothing,
# so it must not list the metadata directory to find the latest metadata file either. An INSERT of one block
# therefore lists as many times with one insert stream as with four.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="05629_iceberg_empty_sinks/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (k Int64) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"

for threads in 1 4; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_metadata_staleness_ms=0 \
        --max_threads=4 --max_insert_threads=${threads} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_${threads}" \
        --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
done

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(k) FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(ProfileEvents['S3ListObjects'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
        AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
