#!/usr/bin/env bash

# In clickhouse-local, a table name written as a path to a file (it contains `.` or `/`) that does not
# exist is reported as a missing file with its resolved path, not as an unknown table identifier.
# https://github.com/ClickHouse/ClickHouse/issues/126441

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
cd "${DIR}" || exit 1
DIR_ABS=$(pwd -P)

printf '1\n2\n' > data.csv

echo '--- an existing file is read'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'data.csv' ORDER BY 1"

echo '--- a missing file with an extension'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'missing.json'" 2>&1 | grep -oF -e 'FILE_DOESNT_EXIST' -e 'UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'missing.json'" 2>&1 | grep -cF "File does not exist: ${DIR_ABS}/missing.json"

echo '--- a missing file in a subdirectory, in back quotes, and in a JOIN'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'sub/missing.csv'" 2>&1 | grep -oF -e 'FILE_DOESNT_EXIST' -e 'UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM \`missing.csv\`" 2>&1 | grep -oF -e 'FILE_DOESNT_EXIST' -e 'UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'data.csv' AS a JOIN 'missing.csv' AS b ON a.c1 = b.c1" 2>&1 | grep -oF -e 'FILE_DOESNT_EXIST' -e 'UNKNOWN_TABLE'

echo '--- a name that is not written as a path is still an unknown table'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM missing_table" 2>&1 | grep -oF -e 'FILE_DOESNT_EXIST' -e 'UNKNOWN_TABLE'
${CLICKHOUSE_LOCAL} -q "SELECT * FROM 'missing'" 2>&1 | grep -oF -e 'FILE_DOESNT_EXIST' -e 'UNKNOWN_TABLE'

cd "${CUR_DIR}" || exit 1
rm -rf "${DIR}"
