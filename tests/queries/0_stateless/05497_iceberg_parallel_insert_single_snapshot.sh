#!/usr/bin/env bash
# Tags: no-fasttest

# All insert streams of a parallel INSERT into an Iceberg table commit one snapshot together,
# with one manifest per partition holding the data files of every stream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_parallel"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
PLAIN_TABLE="t_${CLICKHOUSE_DATABASE}_parallel_plain"
PLAIN_TABLE_PATH="${USER_FILES_PATH}/${PLAIN_TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}; DROP TABLE IF EXISTS ${PLAIN_TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}" "${PLAIN_TABLE_PATH}"
}
trap cleanup EXIT
cleanup

count_manifests()
{
    find "$1metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | wc -l | tr -d ' '
}

count_snapshots()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '$1' AND operation = 'APPEND'"
}

PARALLEL="max_insert_threads = 4, max_threads = 4, max_block_size = 1000"

echo "--- partitioned"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (p Int64, n Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY p"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} SELECT number % 3, number FROM numbers_mt(300000) SETTINGS ${PARALLEL}"
count_snapshots "${TABLE}"
count_manifests "${TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "SELECT p, count(), sum(n) FROM ${TABLE} GROUP BY p ORDER BY p"

echo "--- unpartitioned"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PLAIN_TABLE} (n Int64)
    ENGINE = IcebergLocal('${PLAIN_TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${PLAIN_TABLE} SELECT number FROM numbers_mt(300000) SETTINGS ${PARALLEL}"
count_snapshots "${PLAIN_TABLE}"
count_manifests "${PLAIN_TABLE_PATH}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(n) FROM ${PLAIN_TABLE}"
