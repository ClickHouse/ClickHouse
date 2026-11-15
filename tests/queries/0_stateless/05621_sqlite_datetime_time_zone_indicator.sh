#!/usr/bin/env bash
# Tags: no-fasttest
# SQLite date and time values may end with a time zone indicator (`Z` or `[+-]HH:MM`), see
# https://www.sqlite.org/lang_datefunc.html#time_values. Reading such values into `DateTime`
# must keep working, while a malformed indicator is still rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=${CLICKHOUSE_TMP}/05621_${CLICKHOUSE_DATABASE}.sqlite
rm -f "${DB_PATH}"

sqlite3 "${DB_PATH}" "CREATE TABLE good (d TEXT); INSERT INTO good VALUES ('2024-01-02 03:04:05Z'), ('2024-01-02T03:04:05.123Z'), ('2024-01-02 03:04:05+00:00'), ('2024-01-02 03:04:05.5 -00:00');"
sqlite3 "${DB_PATH}" "CREATE TABLE bad_offset (d TEXT); INSERT INTO bad_offset VALUES ('2024-01-02 03:04:05+0');"
sqlite3 "${DB_PATH}" "CREATE TABLE bad_tail (d TEXT); INSERT INTO bad_tail VALUES ('2024-01-02 03:04:05Zjunk');"

${CLICKHOUSE_LOCAL} --query "CREATE TABLE s (d DateTime('UTC')) ENGINE = SQLite('${DB_PATH}', 'good'); SELECT * FROM s"
${CLICKHOUSE_LOCAL} --query "CREATE TABLE s (d DateTime('UTC')) ENGINE = SQLite('${DB_PATH}', 'bad_offset'); SELECT * FROM s" 2>&1 | grep -o -m1 "CANNOT_PARSE_DATETIME"
${CLICKHOUSE_LOCAL} --query "CREATE TABLE s (d DateTime('UTC')) ENGINE = SQLite('${DB_PATH}', 'bad_tail'); SELECT * FROM s" 2>&1 | grep -o -m1 "CANNOT_PARSE_INPUT_ASSERTION_FAILED"

rm -f "${DB_PATH}"
