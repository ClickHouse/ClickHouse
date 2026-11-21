#!/usr/bin/env bash
# Tags: no-fasttest
# The Iceberg writer splits each chunk by partition with `scatterColumns`. The partition ids are `UInt16`
# when `iceberg_insert_max_partitions` is at most 65536, and `UInt64` above it. Both types must write the
# same rows, with one partition per data file. A chunk over the limit must fail with `BAD_ARGUMENTS` and
# write nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

cleanup()
{
    for max_partitions in 100 100000; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_PREFIX}_${max_partitions}"
        rm -rf "${USER_FILES_PATH}/${TABLE_PREFIX}_${max_partitions}/"
    done
}
trap cleanup EXIT

for max_partitions in 100 100000; do
    TABLE="${TABLE_PREFIX}_${max_partitions}"
    TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${TABLE} (p Int64, k Int64, s String, n Nullable(Int64))
        ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
        PARTITION BY (p)
    "

    # 200 rows in 20 partitions. The limit is checked per chunk, so the rows must reach the writer as one chunk.
    # `arrayJoin` over the one row of `system.one` does that, whatever `max_threads` and `max_block_size` are.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_max_partitions="${max_partitions}" --query "
        INSERT INTO ${TABLE}
        SELECT x % 20, x, toString(x), if(x % 3 = 0, NULL, x) FROM (SELECT arrayJoin(range(200)) AS x)
    "

    echo "iceberg_insert_max_partitions = ${max_partitions}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(k), sum(p * k), count(n), sum(length(s)) FROM ${TABLE}"
    # A row scattered into the wrong piece would put a second partition value into some data file.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT max(partitions_in_file), uniqExact(file_partition)
        FROM (SELECT uniqExact(p) AS partitions_in_file, any(p) AS file_partition FROM ${TABLE} GROUP BY _path)
    "
done

echo "101 partitions at iceberg_insert_max_partitions = 100"
ERROR=$(${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_max_partitions=100 --query "
    INSERT INTO ${TABLE_PREFIX}_100 SELECT x, x, toString(x), x FROM (SELECT arrayJoin(range(101)) AS x)
" 2>&1)
echo "${ERROR}" | grep -o "Too many partitions per insert operation: [0-9]*. The maximum allowed number of partitions is [0-9]*" | head -1
echo "${ERROR}" | grep -o "BAD_ARGUMENTS" | head -1
# The failed insert wrote nothing.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_PREFIX}_100"
