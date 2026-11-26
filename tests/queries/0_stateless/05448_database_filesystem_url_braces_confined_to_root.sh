#!/usr/bin/env bash

# The confinement of the tables of a `Filesystem` database and of the `file://` tables of a `URL`
# database to the directory of the database applies to every path the `{a,b,c}` groups of a name
# expand to: an alternative like `{../dir2,unused}` becomes a `..` component only after the expansion.
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

mkdir -p "${USER_FILES_PATH}/${DIR1}/a" "${USER_FILES_PATH}/${DIR1}/b" "${USER_FILES_PATH}/${DIR2}"
echo 1 > "${USER_FILES_PATH}/${DIR1}/a/data.csv"
echo 2 > "${USER_FILES_PATH}/${DIR1}/b/data.csv"
echo 3 > "${USER_FILES_PATH}/${DIR2}/data.csv"
chmod -R 777 "${USER_FILES_PATH}/${DIR1}" "${USER_FILES_PATH}/${DIR2}"

${CLICKHOUSE_CLIENT} -q "
CREATE DATABASE ${DB_FS} ENGINE = Filesystem('${DIR1}');
CREATE DATABASE ${DB_URL} ENGINE = URL('file://${DIR1}/');
"

echo '--- Filesystem: alternatives beneath the directory'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`{a,b}/data.csv\` ORDER BY ALL"

echo '--- Filesystem: an alternative in a sibling directory'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`{../${DIR2},unused}/data.csv\`" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'

echo '--- URL: alternatives beneath the directory of the base URL'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_URL}.\`{a,b}/data.csv\` ORDER BY ALL"

echo '--- URL: an alternative in a sibling directory'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_URL}.\`file://${DIR1}/{../${DIR2},unused}/data.csv\`" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'
