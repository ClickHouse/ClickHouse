#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/05406_truncate_keeps_multiple_files"
rm -rf "${DIR}"
mkdir -p "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

# A truncating insert replaces only the file of the table, and keeps the files created
# by `engine_file_allow_create_multiple_files`, as it did before the splitting by size existed.
echo '--- engine_file_allow_create_multiple_files, then a truncating insert'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64) ENGINE = File(JSON, '${DIR}/data.json');
    INSERT INTO t VALUES (1);
    INSERT INTO t SETTINGS engine_file_allow_create_multiple_files = 1 VALUES (2);
    INSERT INTO t SETTINGS engine_file_truncate_on_insert = 1 VALUES (3);
    SELECT x FROM t ORDER BY x;
"
ls "${DIR}"
rm -rf "${DIR:?}"/*

# The files written by an insert split by size are deleted by a truncating insert,
# even when it is not split itself, while the other files are kept.
echo '--- engine_file_allow_create_multiple_files, a split insert, then a truncating insert'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64) ENGINE = File(JSON, '${DIR}/data.json');
    INSERT INTO t VALUES (1);
    INSERT INTO t SETTINGS engine_file_allow_create_multiple_files = 1 VALUES (2);
    INSERT INTO t SELECT number + 100 FROM numbers(300) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1000, engine_file_allow_create_multiple_files = 1;
    SELECT count(), uniqExact(_file) > 2 FROM t;
    INSERT INTO t SETTINGS engine_file_truncate_on_insert = 1 VALUES (3);
    SELECT x FROM t ORDER BY x;
"
ls "${DIR}"

rm -rf "${DIR}"
