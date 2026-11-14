#!/usr/bin/env bash
# Tags: no-fasttest
# A multithreaded INSERT into an Iceberg table with a dependent materialized view commits one snapshot:
# the sinks cannot hand their files to one commit through the views, so the insert is written by a single sink.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
DEP="dep_${CLICKHOUSE_DATABASE}_${RANDOM}"
DEP_MV="dep_mv_${CLICKHOUSE_DATABASE}_${RANDOM}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${DEP_MV}; DROP TABLE IF EXISTS ${DEP}; DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (k Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY icebergBucket(2, k)
"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DEP} (k Int64) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${DEP_MV} TO ${DEP} AS SELECT k FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=4 --max_threads=4 --max_block_size=100 \
    --max_insert_block_size=100 --min_insert_block_size_rows=100 --min_insert_block_size_bytes=1 --parallel_view_processing=1 \
    --query "INSERT INTO ${TABLE} SELECT number FROM numbers_mt(10000)"

${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DEP}"

echo "=== snapshots ==="
find "${TABLE_PATH}metadata" -maxdepth 1 -name 'snap-*.avro' -type f | wc -l | tr -d ' '
