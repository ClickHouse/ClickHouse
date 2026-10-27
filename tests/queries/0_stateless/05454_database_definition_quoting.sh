#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no S3

# The `Filesystem`, `S3` and `HDFS` database engines build their definition as text and parse it. They used to put
# the path, the URL and the keys in quotes without escaping them, so with a quote or a backslash in one of them
# `SHOW CREATE DATABASE` and any query that reads `system.databases.engine_full` failed with `SYNTAX_ERROR`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_FS="${CLICKHOUSE_DATABASE}_fs"
DB_S3="${CLICKHOUSE_DATABASE}_s3"
DIR="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}"

# A directory named `x'y\z`.
mkdir -p "${DIR}/x'y\\z"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_FS} ENGINE = Filesystem('${CLICKHOUSE_DATABASE}/x\\'y\\\\z')"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_S3} ENGINE = S3('http://localhost:11111/test/x\\'y\\\\z/', 'key\\'1', 'secret\\'2')"

# The `Filesystem` path is printed absolute; keep only its last component.
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE ${DB_FS} FORMAT TSVRaw" | sed -E "s|Filesystem\('[^']*/x|Filesystem('.../x|"
$CLICKHOUSE_CLIENT -q "SHOW CREATE DATABASE ${DB_S3} FORMAT TSVRaw"
$CLICKHOUSE_CLIENT -q "SELECT name, engine_full FROM system.databases WHERE name IN ('${DB_FS}', '${DB_S3}') ORDER BY name FORMAT TSVRaw" |
    sed -E "s|Filesystem\('[^']*/x|Filesystem('.../x|"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_FS}"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB_S3}"
rm -rf "${DIR:?}"
