#!/usr/bin/env bash
# Tags: no-fasttest
# Writing a zstd-compressed Iceberg metadata file at a high compression level must not allocate
# a zstd context sized for the level (about 836 MiB at level 22) instead of for the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

SETTINGS="--iceberg_metadata_compression_method=zstd --output_format_compression_level=22 --output_format_parquet_compression_method=none --async_insert=0 --allow_experimental_iceberg_compaction=0"

${CLICKHOUSE_CLIENT} ${SETTINGS} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_create" \
    --query "CREATE TABLE ${TABLE} (a Int64, s String) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} ${SETTINGS} --allow_insert_into_iceberg=1 --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_insert" \
    --query "INSERT INTO ${TABLE} SELECT number, toString(number) FROM numbers(10)"

ls "${TABLE_PATH}metadata" | grep 'metadata\.json$' | sort
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "SELECT count(), sum(a) FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''), memory_usage < 500000000
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query_id IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_create', '${CLICKHOUSE_TEST_UNIQUE_NAME}_insert')
        AND type = 'QueryFinish'
    ORDER BY 1"
