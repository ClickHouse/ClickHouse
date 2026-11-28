#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a `Filesystem` database must be a file under the directory of the database,
# not anywhere in `user_files`.

base="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$base/db/sub" "$base/other"
echo 'inner' > "$base/db/inner.csv"
echo 'secret' > "$base/other/secret.csv"

fs_db="${CLICKHOUSE_DATABASE}_fs"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE $fs_db ENGINE = Filesystem('$base/db')"

function query()
{
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -o -m1 -e 'PATH_ACCESS_DENIED' -e '^[a-z01]*$'
}

echo "-- inside"
query "SELECT * FROM $fs_db.\`inner.csv\`"
query "SELECT * FROM $fs_db.\`sub/../inner.csv\`"
query "SELECT * FROM $fs_db.\`{inner,missing}.csv\`"
query "EXISTS TABLE $fs_db.\`inner.csv\`"

echo "-- outside"
query "SELECT * FROM $fs_db.\`../other/secret.csv\`"
query "SELECT * FROM $fs_db.\`$base/other/secret.csv\`"
query "SELECT * FROM $fs_db.\`../other/*.csv\`"
query "SELECT * FROM $fs_db.\`{..,sub}/other/secret.csv\`"
query "SELECT * FROM $fs_db.\`{../other/secret,inner}.csv\`"
query "EXISTS TABLE $fs_db.\`../other/secret.csv\`"
query "EXISTS TABLE $fs_db.\`../other/missing.csv\`"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE $fs_db"
rm -rf "$base"
