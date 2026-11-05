#!/usr/bin/env bash
# Tags: no-fasttest
# An INSERT into Iceberg on S3 uploads finished data files in the background while it writes the next ones.
# Every committed file must be complete (its size in the manifest equals the size of the object), and a failed
# INSERT must remove all the files it wrote, including the ones still being uploaded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="05507_iceberg_uploads/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# One insert stream writing 20 files of 1000 rows, more files than are uploaded together.
SETTINGS="
    SET allow_insert_into_iceberg = 1;
    SET iceberg_insert_max_rows_in_data_file = 1000, iceberg_insert_max_bytes_in_data_file = 536870912;
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 1000, max_insert_block_size = 1000;
    SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
"

# Data files in the manifest, their rows, how many of them exist as objects, and how many differ in size.
check_files()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), sum(record_count), countIf(_file != ''), countIf(_file != '' AND file_size_in_bytes != _size)
        FROM
        (
            SELECT file_path, file_size_in_bytes, record_count
            FROM system.iceberg_files
            WHERE database = currentDatabase() AND table = '$1' AND content = 0
        ) AS manifest
        LEFT JOIN
        (
            SELECT _file, _size FROM s3(s3_conn, filename = '${PREFIX}/$1/data/*.parquet', format = 'One')
        ) AS objects ON basename(manifest.file_path) = objects._file
        SETTINGS join_use_nulls = 0
    "
}

count_objects()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3(s3_conn, filename = '${PREFIX}/$1/data/*.parquet', format = 'One')"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_flat (id Int64) ENGINE = IcebergS3(s3_conn, filename = '${PREFIX}/t_flat');
    CREATE TABLE t_partitioned (p Int64, id Int64) ENGINE = IcebergS3(s3_conn, filename = '${PREFIX}/t_partitioned') PARTITION BY p;
    CREATE TABLE t_sync (id Int64) ENGINE = IcebergS3(s3_conn, filename = '${PREFIX}/t_sync');
"

echo '--- flat ---'
${CLICKHOUSE_CLIENT} --query "${SETTINGS} INSERT INTO t_flat SELECT number FROM numbers(20000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM t_flat"
check_files t_flat

# 10 partitions with 2 files each: the last files of all partitions are uploaded together.
echo '--- partitioned ---'
${CLICKHOUSE_CLIENT} --query "${SETTINGS} INSERT INTO t_partitioned SELECT number % 10, number FROM numbers(20000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id), uniqExact(p) FROM t_partitioned"
check_files t_partitioned

echo '--- failed inserts ---'
${CLICKHOUSE_CLIENT} --query "
    ${SETTINGS} INSERT INTO t_flat SELECT throwIf(number = 15000, 'expected') + number FROM numbers(20000)
" 2>&1 | grep -c -m1 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

# The exception is thrown by the source, so the pipeline is cancelled and the sink's `onException` is not called.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${PREFIX}/bad.csv', format = 'CSV', structure = 's String')
    SELECT if(number = 15000, 'x', toString(number)) FROM numbers(20000)
"
${CLICKHOUSE_CLIENT} --query "
    ${SETTINGS} INSERT INTO t_flat SELECT id FROM s3(s3_conn, filename = '${PREFIX}/bad.csv', format = 'CSV', structure = 'id Int64')
    SETTINGS input_format_parallel_parsing = 0
" 2>&1 | grep -c -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_flat"
count_objects t_flat

echo '--- iceberg_insert_max_data_files_in_upload = 0 ---'
${CLICKHOUSE_CLIENT} --query "${SETTINGS} SET iceberg_insert_max_data_files_in_upload = 0; INSERT INTO t_sync SELECT number FROM numbers(20000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM t_sync"
check_files t_sync

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_flat; DROP TABLE t_partitioned; DROP TABLE t_sync"
