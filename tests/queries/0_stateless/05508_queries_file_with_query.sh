#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_FILE_1="${CLICKHOUSE_TMP}/05508_udf1_$$.sql"
TMP_FILE_2="${CLICKHOUSE_TMP}/05508_udf2_$$.sql"
TMP_FILE_3="${CLICKHOUSE_TMP}/05508_table_$$.sql"
trap 'rm -f "$TMP_FILE_1" "$TMP_FILE_2" "$TMP_FILE_3"' EXIT

# 1. SQL UDF defined in --queries-file and queried with -q
echo "CREATE FUNCTION test_f AS (x) -> x * 2;" > "$TMP_FILE_1"
$CLICKHOUSE_LOCAL --queries-file="$TMP_FILE_1" -q "SELECT test_f(21);"

# 2. Multiple --queries-file executed before query in the same session
echo "CREATE FUNCTION test_g AS (x) -> x + 10;" > "$TMP_FILE_2"
$CLICKHOUSE_LOCAL --queries-file="$TMP_FILE_1" --queries-file="$TMP_FILE_2" -q "SELECT test_f(5) + test_g(5);"

# 3. Table created in --queries-file and queried with -q
echo "CREATE TABLE test_tab (x UInt32) ENGINE = Memory; INSERT INTO test_tab VALUES (1), (2), (3);" > "$TMP_FILE_3"
$CLICKHOUSE_LOCAL --queries-file="$TMP_FILE_3" -q "SELECT sum(x) FROM test_tab;"

# 4. Multiple queries with -q after --queries-file
$CLICKHOUSE_LOCAL --queries-file="$TMP_FILE_1" -q "SELECT test_f(1);" -q "SELECT test_f(2);"
