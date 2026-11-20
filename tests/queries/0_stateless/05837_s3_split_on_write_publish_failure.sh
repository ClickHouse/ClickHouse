#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: Depends on S3
# Tag no-parallel: The failpoint is global for the server, and a concurrent split insert could trigger it

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An object of an insert split by size is committed before it is registered in the table. If the registration fails,
# the object is removed: otherwise a glob pattern over the prefix would read it, and the next truncating insert
# would not delete it either, as it deletes only the keys the table knows.

PREFIX="05837_split_publish_failure/${CLICKHOUSE_DATABASE}"

# Every block is exactly 100 numbers, and a new object is started as soon as 1000 bytes are written,
# so the resulting objects are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, s3_split_on_write_by_size_bytes = 1000"

# The failpoint fails the registration of a new key when the table already lists two: here, `data.2.tsv`.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_05837 (x UInt64) ENGINE = S3(s3_conn, filename='${PREFIX}/data.tsv', format=TSV);

    SELECT '--- A split insert writes two objects and fails to register the third one';
    SYSTEM ENABLE FAILPOINT split_on_write_fail_to_publish_third_path;
    INSERT INTO test_05837 SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}; -- { serverError FAULT_INJECTED }
    SYSTEM DISABLE FAILPOINT split_on_write_fail_to_publish_third_path;
    SELECT _file, count() FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- A smaller truncating insert leaves only its own rows';
    INSERT INTO test_05837 SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, s3_truncate_on_insert = 1;
    SELECT _file, count(), min(x), max(x) FROM s3(s3_conn, filename='${PREFIX}/data*.tsv', format=TSV, structure='x UInt64') GROUP BY _file ORDER BY _file;
    SELECT count(), min(x), max(x) FROM test_05837;

    DROP TABLE test_05837;
"
