#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A file of a split insert becomes a part of the table only after it has been written. If the insert fails while
# writing a file, the file is removed: otherwise a glob pattern over the directory would read it, and the next
# truncating insert would not delete it either, as it deletes only the files the table knows.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# Every block is exactly 100 numbers, and a new file is started as soon as 1000 bytes are written,
# so the resulting files are the same on every run: 0..299 go to `data.tsv`, 300..599 to `data.1.tsv`,
# 600..899 to `data.2.tsv`, and `data.3.tsv` is started with 900..999 and 1000..1099.
SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(TSV, '${DIR}/data.tsv')"

echo '--- The insert fails while writing the fourth file'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT throwIf(number = 1150) + number FROM numbers(1200) SETTINGS ${SETTINGS}" 2>&1 | grep -o -m1 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

# The insert holds the lock of the table until its sink is gone, so this waits for the cleanup as well.
echo '--- The table reads the files that were finished, and the unfinished one is removed'
${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM test"
ls "${DIR}" | sort

echo '--- A smaller truncating insert leaves no file of the failed insert behind'
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(100) SETTINGS ${SETTINGS}, engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM test"
${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM file('${DIR}/data*.tsv', TSV, 'x UInt64')"
ls "${DIR}" | sort

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
rm -rf "${DIR}"
