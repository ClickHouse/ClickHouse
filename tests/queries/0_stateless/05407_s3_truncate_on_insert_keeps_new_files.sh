#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="05407_truncate_keeps_new_files/${CLICKHOUSE_DATABASE}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

# A truncating insert that is not split replaces only the last object of the table, and keeps the other
# objects created by `s3_create_new_file_on_insert`, as it did before the splitting by size existed.
echo '--- s3_create_new_file_on_insert, then a truncating insert'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05407 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/plain/data.tsv', format=TSV);
    INSERT INTO test_05407 VALUES (1);
    INSERT INTO test_05407 SETTINGS s3_create_new_file_on_insert = 1 VALUES (2);
    INSERT INTO test_05407 SETTINGS s3_truncate_on_insert = 1 VALUES (3);
    SELECT x FROM test_05407 ORDER BY x;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/plain/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    DROP TABLE test_05407;
"

# The objects written by an insert split by size are deleted by a truncating insert,
# even when it is not split itself, while the other objects are kept.
echo '--- s3_create_new_file_on_insert, a split insert, then a truncating insert'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05407 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/split/data.tsv', format=TSV);
    INSERT INTO test_05407 VALUES (1);
    INSERT INTO test_05407 SETTINGS s3_create_new_file_on_insert = 1 VALUES (2);
    INSERT INTO test_05407 SELECT number + 100 FROM numbers(300) SETTINGS ${SETTINGS}, s3_split_on_write_by_size_bytes = 1000, s3_create_new_file_on_insert = 1;
    SELECT count(), uniqExact(_file) > 2 FROM test_05407;
    INSERT INTO test_05407 SETTINGS s3_truncate_on_insert = 1 VALUES (3);
    SELECT x FROM test_05407 ORDER BY x;
    SELECT _file FROM s3(s3_conn, filename='${PREFIX}/split/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    DROP TABLE test_05407;
"
