#!/usr/bin/env bash

# The tables of a `Filesystem` database, and the `file://` tables of a `URL` database with a `file://`
# base URL, are the files beneath the directory of the database: a name must not reach another
# directory of `user_files` through `..` or an absolute path. `clickhouse-local` accepts any path.
# https://github.com/ClickHouse/ClickHouse/pull/100173

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR1="${CLICKHOUSE_TEST_UNIQUE_NAME}_dir1"
DIR2="${CLICKHOUSE_TEST_UNIQUE_NAME}_dir2"
DB_FS="db_fs_${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_URL="db_url_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_FS}; DROP DATABASE IF EXISTS ${DB_URL};"
    rm -rf "${USER_FILES_PATH:?}/${DIR1}" "${USER_FILES_PATH:?}/${DIR2}"
}
trap cleanup EXIT

mkdir -p "${USER_FILES_PATH}/${DIR1}/nested" "${USER_FILES_PATH}/${DIR2}"
echo 1 > "${USER_FILES_PATH}/${DIR1}/data.csv"
echo 2 > "${USER_FILES_PATH}/${DIR1}/nested/data.csv"
echo 3 > "${USER_FILES_PATH}/${DIR2}/data.csv"
chmod -R 777 "${USER_FILES_PATH}/${DIR1}" "${USER_FILES_PATH}/${DIR2}"

${CLICKHOUSE_CLIENT} -q "
CREATE DATABASE ${DB_FS} ENGINE = Filesystem('${DIR1}');
CREATE DATABASE ${DB_URL} ENGINE = URL('file://${DIR1}/');
"

echo '--- Filesystem: files beneath the directory'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`data.csv\`"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`nested/../nested/data.csv\`"

echo '--- Filesystem: a sibling directory'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`../${DIR2}/data.csv\`" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`${USER_FILES_PATH}/${DIR2}/data.csv\`" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB_FS}.\`../${DIR2}/data.csv\`" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

echo '--- URL: files beneath the directory of the base URL'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_URL}.\`data.csv\`"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_URL}.\`nested/data.csv\`"

echo '--- URL: a sibling directory'
# The resolution against the base URL does not go above the root of the base path (RFC 3986).
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_URL}.\`../${DIR2}/data.csv\`" 2>&1 | grep -q '^3$' && echo 'read' || echo 'not read'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_URL}.\`file://${DIR2}/data.csv\`" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

echo '--- clickhouse-local accepts any path'
${CLICKHOUSE_LOCAL} -q "
CREATE DATABASE fs ENGINE = Filesystem('${USER_FILES_PATH}/${DIR1}');
SELECT * FROM fs.\`../${DIR2}/data.csv\`;
"
