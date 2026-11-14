#!/usr/bin/env bash
# Tags: no-fasttest
# A multithreaded INSERT into Iceberg commits the files of all its parallel sinks in one snapshot, not one per sink,
# with one manifest per partition holding the files of every sink.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null 2>&1
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (k Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY icebergBucket(2, k)
"

# Small blocks spread the insert over all the parallel sinks. The data file limits are above the insert size,
# so each sink writes at most one file per partition: more than two files in a snapshot mean several sinks.
SETTINGS="--allow_insert_into_iceberg=1 --max_insert_threads=4 --max_threads=4 --max_block_size=100
    --max_insert_threads_min_free_memory_per_thread=0 --max_threads_min_free_memory_per_thread=0
    --max_insert_block_size=100 --min_insert_block_size_rows=100 --min_insert_block_size_bytes=1
    --iceberg_insert_max_rows_in_data_file=1000000 --iceberg_insert_max_bytes_in_data_file=1000000000"

echo "=== INSERT SELECT ==="
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "INSERT INTO ${TABLE} SELECT number FROM numbers_mt(10000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${TABLE}"
echo "manifests: $(find "${TABLE_PATH}metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | wc -l | tr -d ' ')"

echo "=== INSERT with data from the client ==="
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(10000, 5000) FORMAT TSV" \
    | ${CLICKHOUSE_CLIENT} ${SETTINGS} --query "INSERT INTO ${TABLE} FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${TABLE}"

echo "=== one snapshot per INSERT ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT
        operation,
        summary['added-records'],
        summary['changed-partition-count']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at
"

echo "=== several sinks in the INSERT SELECT snapshot ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt64(summary['added-data-files']) > 2
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY made_current_at
    LIMIT 1
"
