#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses the `Parquet` format

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the `INSERT` structure-mismatch diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

echo "-- several inserts read one piped stdin: each continues where the previous one stopped"
printf '(1);(2);' | $CLICKHOUSE_LOCAL \
    --query 'CREATE TABLE t (x UInt8) ENGINE = Memory' \
    --query 'INSERT INTO t FORMAT Values' \
    --query 'INSERT INTO t FORMAT Values' \
    --query 'SELECT arraySort(groupArray(x)) FROM t'

echo "-- the same with a regular file as stdin"
printf '(1);(2);' > "${DATA_FILE}.values"
$CLICKHOUSE_LOCAL \
    --query 'CREATE TABLE t (x UInt8) ENGINE = Memory' \
    --query 'INSERT INTO t FORMAT Values' \
    --query 'INSERT INTO t FORMAT Values' \
    --query 'SELECT arraySort(groupArray(x)) FROM t' < "${DATA_FILE}.values"

echo "-- Parquet from a regular file as stdin"
$CLICKHOUSE_LOCAL --query "SELECT number AS x FROM numbers(1000) FORMAT Parquet" > "${DATA_FILE}.parquet"
$CLICKHOUSE_LOCAL \
    --query "CREATE TABLE t (x UInt64) ENGINE = Memory" \
    --query "INSERT INTO t FORMAT Parquet" \
    --query "SELECT count(), sum(x) FROM t" < "${DATA_FILE}.parquet"

echo "-- a genuine mismatch in a regular file as stdin is explained"
printf '1\ttext\n' > "${DATA_FILE}.tsv"
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV" < "${DATA_FILE}.tsv" 2>&1 | check

echo "-- MySQLDump with a declared schema: only the fractional value is invalid"
printf "CREATE TABLE t (s int, n int);\nLOCK TABLES t WRITE;\nINSERT INTO t VALUES ('1', 1.5);\n" \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (s String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- schema_inference_hints do not manufacture a mismatch"
printf '{"a":1,"n":1.5}\n' \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a UInt8, n UInt8) ENGINE = Memory; INSERT INTO t SETTINGS schema_inference_hints = 'a Array(UInt8)' FORMAT JSONEachRow" 2>&1 | check

echo "-- a valid nested value beside an unrelated failing field"
printf "['00000000-0000-0000-0000-000000000000']\t1.5\n" \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Array(UUID), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV" 2>&1 | check
$CLICKHOUSE_LOCAL --query "SELECT [toDate32('1970-01-02')] AS a, 'x' AS u FORMAT Native" > "${DATA_FILE}.native"
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Array(UInt8), u UUID) ENGINE = Memory; INSERT INTO t FORMAT Native" < "${DATA_FILE}.native" 2>&1 | check

echo "-- a nested value with text where numbers are expected is still explained"
printf "['abc']\t1\n" \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV" 2>&1 | check

echo "-- a quoted Bool beside an unrelated failing field"
$CLICKHOUSE_LOCAL --query "SET input_format_values_deduce_templates_of_expressions = 0; CREATE TABLE t (b Bool, d Date) ENGINE = Memory; INSERT INTO t VALUES ('true', 'bad-date')" < /dev/null 2>&1 | check
printf "INSERT INTO t VALUES ('true', 'bad-date');\n" \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (b Bool, d Date) ENGINE = Memory; INSERT INTO t FORMAT MySQLDump" 2>&1 | check

echo "-- a quoted text that is not a Bool is still explained"
$CLICKHOUSE_LOCAL --query "SET input_format_values_deduce_templates_of_expressions = 0; CREATE TABLE t (b Bool, d Date) ENGINE = Memory; INSERT INTO t VALUES ('hello', 'bad-date')" < /dev/null 2>&1 | check

echo "-- aggregate_function_input_format: compared with the values the format parses"
printf '[1,2]\t1.5\n' > "${DATA_FILE}.agg.tsv"
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (a AggregateFunction(sum, UInt8), b UInt8) ENGINE = Memory; INSERT INTO t FROM INFILE '${DATA_FILE}.agg.tsv' SETTINGS aggregate_function_input_format = 'array' FORMAT TSV" < /dev/null 2>&1 | check
printf '[1,2]\t1.5\n' \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a AggregateFunction(sum, UInt8), b UInt8) ENGINE = Memory; INSERT INTO t SETTINGS aggregate_function_input_format = 'array' FORMAT TSV" 2>&1 | check
printf '1\ttext\n' \
    | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a AggregateFunction(sum, UInt8), b UInt8) ENGINE = Memory; INSERT INTO t SETTINGS aggregate_function_input_format = 'value' FORMAT TSV" 2>&1 | check

rm -f "${DATA_FILE}".*
