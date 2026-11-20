#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

CLIENT="${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=1 --allow_insert_into_iceberg=1"

${CLIENT} --query "CREATE TABLE ${TABLE} (c0 Nullable(Int64)) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
${CLIENT} --query "SELECT sum(c0) FROM ${TABLE}"

${CLIENT} --query "ALTER TABLE ${TABLE} MODIFY COLUMN c0 Int64" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

${CLIENT} --query "SELECT sum(c0) FROM ${TABLE}"
${CLIENT} --query "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '${TABLE}'"

${CLIENT} --query "DROP TABLE ${TABLE}"
