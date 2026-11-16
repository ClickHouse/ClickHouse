#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Iceberg reads a Parquet data file in order only where the order is needed: a file whose position deletes are
# streamed, and the read of `ALTER TABLE ... DELETE`, which writes the positions of every data file in read order.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
THREADS="max_threads = 16, max_parsing_threads = 16"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64, s String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 1000
"

# Three data files of many row groups each.
for i in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO ${TABLE} SELECT number + ${i} * 100000, toString(number) FROM numbers(100000)
        SETTINGS allow_insert_into_iceberg = 1, max_insert_threads = 1"
done

# Position delete files written in both position-delete modes.
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DELETE WHERE id % 2 = 0
    SETTINGS allow_insert_into_iceberg = 1, use_roaring_bitmap_iceberg_positional_deletes = 0, ${THREADS}"
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DELETE WHERE id % 1000 = 3
    SETTINGS allow_insert_into_iceberg = 1, use_roaring_bitmap_iceberg_positional_deletes = 1, ${THREADS}"

# A data file without position deletes.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO ${TABLE} SELECT number + 300000, toString(number) FROM numbers(100000)
    SETTINGS allow_insert_into_iceberg = 1, max_insert_threads = 1"

echo "--- positions of every data file increase in the delete files"
find "${TABLE_PATH}" -name "*-deletes.parquet" | sort | while read -r delete_file; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT count(), min(sorted)
        FROM
        (
            SELECT file_path, arrayMap(x -> x.2, arraySort(groupArray((_row_number, pos)))) = arraySort(groupArray(pos)) AS sorted
            FROM file('${delete_file}', 'Parquet')
            GROUP BY file_path
        )"
done

echo "--- deletes apply"
for roaring in 0 1; do
    for threads in 1 16; do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT count(), sum(id), min(id), countIf(id % 2 = 0 OR id % 1000 = 3) FROM ${TABLE}
            SETTINGS use_roaring_bitmap_iceberg_positional_deletes = ${roaring}, max_threads = ${threads}, max_parsing_threads = ${threads}"
    done
done

echo "--- streamed deletes apply in every read"
# A delete file read out of order loses deletes only in some reads, most often in a read of no columns.
for _ in {1..10}; do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} SETTINGS use_roaring_bitmap_iceberg_positional_deletes = 0, ${THREADS}"
done | sort | uniq -c

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
