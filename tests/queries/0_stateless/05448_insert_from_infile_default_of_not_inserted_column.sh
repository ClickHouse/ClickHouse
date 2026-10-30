#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# INSERT (columns) FROM INFILE: a not inserted column in a DEFAULT expression takes its own default, as with inline data.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a UInt32 DEFAULT 5, b UInt32 DEFAULT a * 2, c UInt32 DEFAULT b + 1) ENGINE = MergeTree ORDER BY tuple();
"

echo '{"a":[6,7],"b":[null,3]}' > "${DATA_FILE}.json"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t (b) FROM INFILE '${DATA_FILE}.json' FORMAT JSONColumns"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t (b) FORMAT JSONColumns {\"a\":[6,7],\"b\":[null,4]}"

printf '{"b":null}\n{"b":5}\n' > "${DATA_FILE}.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t (b) FROM INFILE '${DATA_FILE}.jsonl' FORMAT JSONEachRow"

# Chain: c -> b -> a, neither "b" nor "a" is inserted.
echo '{"c":[null,100]}' > "${DATA_FILE}_c.json"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t (c) FROM INFILE '${DATA_FILE}_c.json' FORMAT JSONColumns"

$CLICKHOUSE_CLIENT -q "SELECT a, b, c FROM t ORDER BY b, c"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# A lambda argument with the name of a not inserted column is not replaced.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_lambda (a UInt32 DEFAULT 5, b UInt32 DEFAULT arraySum(arrayMap(a -> a + 1, [1, 2])) + a) ENGINE = Memory"
printf '{"b":null}\n' > "${DATA_FILE}_lambda.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_lambda (b) FROM INFILE '${DATA_FILE}_lambda.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM t_lambda"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_lambda"

# The default of a not inserted column is substituted into a lambda, whose argument has the name of an inserted column:
# the inserted column is not captured by the lambda argument. The same as the inline insert: 10, 20, 43.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_capture (z UInt32, a UInt32 DEFAULT z * 2, b UInt32 DEFAULT arraySum(arrayMap(z -> a + z, [1, 2]))) ENGINE = Memory"
printf '{"z":10,"b":null}\n' > "${DATA_FILE}_capture.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_capture (z, b) FROM INFILE '${DATA_FILE}_capture.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_capture (z, b) FORMAT JSONEachRow {\"z\":10,\"b\":null}"
$CLICKHOUSE_CLIENT -q "SELECT z, a, b FROM t_capture"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_capture"

rm -f "${DATA_FILE}.json" "${DATA_FILE}.jsonl" "${DATA_FILE}_c.json" "${DATA_FILE}_lambda.jsonl" "${DATA_FILE}_capture.jsonl"
