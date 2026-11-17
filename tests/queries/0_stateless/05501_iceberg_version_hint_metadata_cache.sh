#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-parallel-replicas: counts the version hint reads of the initial query, which with parallel replicas resolves the table again for the query it sends to the replicas

# With `iceberg_use_version_hint`, the current metadata file is resolved through the latest-metadata-version
# cache like a listed or a catalog-provided one, so with `iceberg_metadata_staleness_ms` a hot read neither
# reads `metadata/version-hint.text` nor lists the metadata directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="05501_iceberg_version_hint_metadata_cache/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')
    SETTINGS iceberg_use_version_hint = 1
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

run_select()
{
    ${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms="$1" --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_$2" \
        --query "SELECT '$2', count(), sum(c0) FROM ${TABLE}"
}

run_select 600000 warm
run_select 600000 stale
run_select 0 fresh

# An external writer resets the hint to the first (empty) version: a read within the staleness window
# keeps the cached version, a fresh one follows the hint.
${CLICKHOUSE_CLIENT} --s3_truncate_on_insert=1 --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${TABLE_PATH}/metadata/version-hint.text', format = 'RawBLOB') SELECT '1'
"
run_select 600000 stale_after_external_change
run_select 0 fresh_after_external_change

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''), ProfileEvents['IcebergVersionHintUsed'], ProfileEvents['S3ListObjects']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'
    ORDER BY event_time_microseconds
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
