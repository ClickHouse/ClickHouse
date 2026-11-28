#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 129159: ALTER TABLE ... DELETE and UPDATE on Iceberg write one position delete file per data file, with
# positions in increasing order, so a read applies each delete file only to its own data file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# 500 rows per data file.
FILL="SETTINGS iceberg_insert_max_rows_in_data_file = 500, max_insert_threads = 1, max_block_size = 500,
    min_insert_block_size_rows = 500, min_insert_block_size_bytes = 0"
# Parallel reads in small blocks interleave the deleted rows of many data files.
MUTATE="SETTINGS max_threads = 8, max_block_size = 64"

# Prints the number of delete files, how many reference exactly one data file and how many have sorted positions.
delete_files()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(paths = 1), countIf(sorted)
        FROM
        (
            SELECT uniqExact(file_path) AS paths, groupArray(pos) = arraySort(groupArray(pos)) AS sorted
            FROM file('${LAKE}/$1/data/*-deletes.parquet', Parquet)
            GROUP BY _file
        )
        SETTINGS input_format_parquet_preserve_order = 1, max_threads = 1"
}

# Runs the query, then prints its pruned and applied (data file, delete file) pairs.
pairs()
{
    local query_id="${CLICKHOUSE_DATABASE}_$(random_str 10)"
    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" --query "$1"
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT 'pairs', ProfileEvents['IcebergMinMaxPrunedDeleteFiles'], ProfileEvents['IcebergMinMaxNonPrunedDeleteFiles']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish'"
}

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE t (n Int64, s String) ENGINE = IcebergLocal('${LAKE}/t/', 'Parquet');
    INSERT INTO t SELECT number, toString(number) FROM numbers(5000) ${FILL};
    SELECT 'data files', count() FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't';
    ALTER TABLE t DELETE WHERE n % 7 = 0 ${MUTATE};
    SELECT 'position delete files', count(), sum(record_count)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't' AND content = 'POSITION_DELETE';
"
delete_files t
echo "expected: $(${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(number) FROM numbers(5000) WHERE number % 7 != 0")"
pairs "SELECT count(), sum(n) FROM t"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(n) FROM t SETTINGS use_roaring_bitmap_iceberg_positional_deletes = 1"

echo '--- a failed UPDATE removes the delete files it wrote'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    ALTER TABLE t UPDATE s = toString(throwIf(n = 10)) WHERE n % 5 = 0 ${MUTATE}" 2>&1 \
    | grep -oF 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' | head -n1
delete_files t
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(n), countIf(s = 'x') FROM t"

echo '--- UPDATE writes a delete file per data file for the old rows, and one data file for the new ones'
# A Replicated database runs the ALTER once per shard on the same files, so a second run must match no rows.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    ALTER TABLE t UPDATE s = 'x' WHERE n % 5 = 0 AND s != 'x' ${MUTATE};
    SELECT content, count(), sum(record_count)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't' GROUP BY content ORDER BY content;
"
delete_files t
echo "expected: $(${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(number), countIf(number % 5 = 0) FROM numbers(5000) WHERE number % 7 != 0")"
pairs "SELECT count(), sum(n), countIf(s = 'x') FROM t"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(n), countIf(s = 'x') FROM t SETTINGS use_roaring_bitmap_iceberg_positional_deletes = 1"

echo '--- partitioned: 2 data files per partition'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE tp (n Int64, p Int64) ENGINE = IcebergLocal('${LAKE}/tp/', 'Parquet') PARTITION BY (p);
    INSERT INTO tp SELECT number, number % 2 FROM numbers(2000) ${FILL};
    ALTER TABLE tp DELETE WHERE n % 7 = 0 ${MUTATE};
    SELECT partition, content, count(), sum(record_count)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'tp' GROUP BY ALL ORDER BY ALL;
"
delete_files tp
echo "expected: $(${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(number) FROM numbers(2000) WHERE number % 7 != 0")"
pairs "SELECT count(), sum(n) FROM tp"

rm -rf "${LAKE}"
