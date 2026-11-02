#!/usr/bin/env bash

# A PUT upload into a table named `function` must not be parsed as `INSERT INTO FUNCTION ...`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.\`function\` (a UInt32) ENGINE = Memory"

echo "-- unqualified upload"
printf '1\n' | curl -sS -X PUT --data-binary @- \
    "${BASE_URL}/function.CSV?database=${DB}&http_allow_database_as_path=0&http_allow_table_as_file=1"

echo "-- database-prefixed upload"
printf '2\n' | curl -sS -X PUT --data-binary @- \
    "${BASE_URL}/${DB}/function.CSV?http_allow_database_as_path=1&http_allow_table_as_file=1"

echo "-- inserted rows"
${CLICKHOUSE_CLIENT} -q "SELECT a FROM ${DB}.\`function\` ORDER BY a"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DB}.\`function\`"
