#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A file of an insert split by size is complete before it is registered in the table. If the registration fails,
# the file is removed: otherwise a glob pattern over the directory would read it, and the next truncating insert
# would not delete it either, as it deletes only the files the table knows.

DIR="${CLICKHOUSE_TMP}/05836_split_publish_failure"
rm -rf "${DIR}"
mkdir -p "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

# The failpoint is local to the process, so the whole scenario runs in a single `clickhouse-local`.
# It fails the registration of a new file when the table already lists two: here, `data.2.tsv`.
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv');

    SELECT '--- A split insert writes two files and fails to register the third one';
    SYSTEM ENABLE FAILPOINT split_on_write_fail_to_publish_third_path;
    INSERT INTO test SELECT number FROM numbers(1000) SETTINGS ${SETTINGS}; -- { serverError FAULT_INJECTED }
    SYSTEM DISABLE FAILPOINT split_on_write_fail_to_publish_third_path;
    SELECT _file, count() FROM file('${DIR}/data*.tsv', TSV, 'x UInt64') GROUP BY _file ORDER BY _file;

    SELECT '--- A smaller truncating insert leaves only its own rows';
    INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1;
    SELECT _file, count(), min(x), max(x) FROM file('${DIR}/data*.tsv', TSV, 'x UInt64') GROUP BY _file ORDER BY _file;
    SELECT count(), min(x), max(x) FROM test;
"

rm -rf "${DIR}"
