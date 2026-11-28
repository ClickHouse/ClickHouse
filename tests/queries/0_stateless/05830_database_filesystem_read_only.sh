#!/usr/bin/env bash

# The tables of a `Filesystem` database are read-only: every write through them is refused, and the
# files are left as they were.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

fs_db="db_05830_${CLICKHOUSE_DATABASE}_$RANDOM"
dir="${USER_FILES_PATH}/d_05830_${CLICKHOUSE_DATABASE}_$RANDOM"

mkdir -p "$dir"
echo 1 > "$dir/a.csv"
echo 1 > "$dir/b.csv"

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS $fs_db;
CREATE DATABASE $fs_db ENGINE = Filesystem('$dir');
CREATE TABLE ${CLICKHOUSE_DATABASE}.src (c1 Nullable(Int64)) ENGINE = Memory;
CREATE MATERIALIZED VIEW ${CLICKHOUSE_DATABASE}.mv TO $fs_db.\`a.csv\` (c1 Nullable(Int64)) AS SELECT c1 FROM ${CLICKHOUSE_DATABASE}.src;
EOF

function refused()
{
    (( $(${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -c "readonly mode") >= 1 )) && echo "refused" || echo "UNEXPECTED"
}

# Reads work. This also caches the table for `a.csv`, while `b.csv` stays uncached.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $fs_db.\`a.csv\`"

refused "INSERT INTO $fs_db.\`a.csv\` VALUES (2)"
# The refusal reaches the user, not only the background flush.
refused "INSERT INTO $fs_db.\`a.csv\` SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (3)"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE $fs_db.\`a.csv\`"
# A renaming rule resolves the table outside the cache.
refused "INSERT INTO $fs_db.\`a.csv\` SETTINGS rename_files_after_processing = '%f.bak' VALUES (4)"
refused "TRUNCATE TABLE $fs_db.\`b.csv\`"
refused "INSERT INTO ${CLICKHOUSE_DATABASE}.src SETTINGS materialized_views_ignore_errors = 0 VALUES (5)"

cat "$dir/a.csv" "$dir/b.csv"
ls "$dir"

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW ${CLICKHOUSE_DATABASE}.mv;
DROP TABLE ${CLICKHOUSE_DATABASE}.src;
DROP DATABASE $fs_db;
EOF

# Remove the directory only once the database that points at it is gone.
if [[ "$(${CLICKHOUSE_CLIENT} --query "EXISTS DATABASE $fs_db" 2>/dev/null)" == "0" ]]
then
    rm -rd "$dir"
fi
