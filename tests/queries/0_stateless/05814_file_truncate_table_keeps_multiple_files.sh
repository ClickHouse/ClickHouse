#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/05814_truncate_table_keeps_multiple_files"
rm -rf "${DIR}"
mkdir -p "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0"

# With the splitting by size disabled throughout, `TRUNCATE TABLE` truncates the files created
# by `engine_file_allow_create_multiple_files` in place, as it always did, rather than deleting them:
# this needs only the permission to write the files, not the directory.
echo '--- engine_file_allow_create_multiple_files only'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64) ENGINE = File(JSON, '${DIR}/data.json');
    INSERT INTO t VALUES (1);
    INSERT INTO t SETTINGS engine_file_allow_create_multiple_files = 1 VALUES (2);
    INSERT INTO t SETTINGS engine_file_allow_create_multiple_files = 1 VALUES (3);
    SELECT x FROM t ORDER BY x;
    TRUNCATE TABLE t;
    SELECT count() FROM t;
"
for f in $(ls "${DIR}" | sort); do echo "$f $(wc -c < "${DIR}/$f")"; done
rm -rf "${DIR:?}"/*

# The files written by an insert split by size are deleted, the other ones are truncated in place.
echo '--- engine_file_allow_create_multiple_files, then a split insert'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t2 (x UInt64) ENGINE = File(JSON, '${DIR}/data.json');
    INSERT INTO t2 VALUES (1);
    INSERT INTO t2 SETTINGS engine_file_allow_create_multiple_files = 1 VALUES (2);
    INSERT INTO t2 SELECT number + 100 FROM numbers(300) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1000, engine_file_allow_create_multiple_files = 1;
    SELECT count(), uniqExact(_file) > 2 FROM t2;
    TRUNCATE TABLE t2;
    SELECT count() FROM t2;
"
for f in $(ls "${DIR}" | sort); do echo "$f $(wc -c < "${DIR}/$f")"; done

rm -rf "${DIR}"
