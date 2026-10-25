#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `JSON` is a format that cannot be appended to, so a second partitioned insert with `engine_file_allow_create_multiple_files`
# steps aside from the non-empty file of the partition into a new numbered one. If the insert fails before that file is
# finished - here, before the first rollover of the split - the file is removed, as with a non-partitioned insert:
# otherwise a glob pattern over the directory would read an incomplete file.

DIR="${CLICKHOUSE_TMP}/05410_split_on_write_partitioned_aborted"
rm -rf "${DIR}"
mkdir -p "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, engine_file_split_on_write_by_size_bytes = 1000000000, engine_file_allow_create_multiple_files = 1"
FILES="file('${DIR}/p_*.json', JSON, 'p UInt64, x UInt64')"

echo '--- The first insert'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}_part.json', JSON, 'p UInt64, x UInt64') PARTITION BY p
        SELECT 0 AS p, number AS x FROM numbers(100) SETTINGS ${SETTINGS};
"
ls "${DIR}" | LC_ALL=C sort

echo '--- The second insert fails in its second block, while writing the new numbered file'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}_part.json', JSON, 'p UInt64, x UInt64') PARTITION BY p
        SELECT 0 AS p, number + throwIf(number = 250) AS x FROM numbers(100, 200) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -m1 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'

echo '--- Only the completed file remains'
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x) FROM ${FILES} GROUP BY p ORDER BY p"

echo '--- The next insert succeeds and steps aside into the same numbered file'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}_part.json', JSON, 'p UInt64, x UInt64') PARTITION BY p
        SELECT 0 AS p, number AS x FROM numbers(100, 100) SETTINGS ${SETTINGS};
"
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, count(), sum(x) FROM ${FILES} GROUP BY p ORDER BY p"

rm -rf "${DIR}"
