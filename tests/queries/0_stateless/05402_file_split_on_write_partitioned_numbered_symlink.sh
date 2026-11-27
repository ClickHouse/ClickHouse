#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A partitioned insert with `engine_file_allow_create_multiple_files` steps aside from a non-empty file into a numbered
# name. The number is placed into the path pattern before the partition id is substituted, so it can change a directory
# that a `..` of the partition id leads out of: with `p.{_partition_id}.parquet` and the partition id `x/../target`, the
# first path is `p.x/../target.parquet`, and the numbered one is `p.1.x/../target.parquet`. If `p.1.x` is a symlink to a
# directory outside of `user_files`, the numbered path leads out of it, so every numbered path is checked like the first one.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
OUTSIDE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_outside"
rm -rf "${DIR}" "${OUTSIDE}"
mkdir -p "${DIR}/p.x" "${OUTSIDE}/sub"
chmod -R 777 "${DIR}" "${OUTSIDE}"
ln -s "${OUTSIDE}/sub" "${DIR}/p.1.x"

PATTERN="${CLICKHOUSE_TEST_UNIQUE_NAME}/p.{_partition_id}.parquet"

echo '--- The first path stays inside user_files'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PATTERN}', Parquet, 'p String, x UInt64') PARTITION BY p SELECT 'x/../target', 1;
"
ls "${DIR}" | LC_ALL=C sort

echo '--- The numbered path that leads out of user_files is refused'
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PATTERN}', Parquet, 'p String, x UInt64') PARTITION BY p SELECT 'x/../target', 2
        SETTINGS engine_file_allow_create_multiple_files = 1;
" 2>&1 | grep -o -m1 'DATABASE_ACCESS_DENIED'
ls "${OUTSIDE}"

rm -rf "${DIR}" "${OUTSIDE}"
