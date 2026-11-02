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

# The new name of a lambda argument does not capture an alias of the expression. The same as the inline insert: 5, 33.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_alias_capture (a UInt32 DEFAULT 5, b UInt32 DEFAULT arraySum(arrayMap(x -> x + __lambda_argument_0, [1, 2])) + (10 AS __lambda_argument_0)) ENGINE = Memory"
printf '{"b":null}\n' > "${DATA_FILE}_alias_capture.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_alias_capture (b) FROM INFILE '${DATA_FILE}_alias_capture.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_alias_capture (b) FORMAT JSONEachRow {\"b\":null}"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM t_alias_capture"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_alias_capture"

# The alias of a substituted not inserted column is kept. The same as the inline insert: 5, 10.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_alias (a UInt32 DEFAULT 5, b UInt32 DEFAULT (a AS x) + x) ENGINE = Memory"
printf '{"b":null}\n' > "${DATA_FILE}_alias.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_alias (b) FROM INFILE '${DATA_FILE}_alias.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_alias (b) FORMAT JSONEachRow {\"b\":null}"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM t_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_alias"

# A default which is used many times is defined once, not copied for every use: "a15" uses "a0" 2^15 times.
COLUMNS="a0 UInt64 DEFAULT 1"
for i in $(seq 1 15); do COLUMNS="${COLUMNS}, a${i} UInt64 DEFAULT a$((i - 1)) + a$((i - 1))"; done
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_chain (${COLUMNS}) ENGINE = Memory"
printf '{"a15":42}\n{"a15":null}\n' > "${DATA_FILE}_chain.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_chain (a15) FROM INFILE '${DATA_FILE}_chain.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_chain (a15) FORMAT JSONEachRow {\"a15\":42} {\"a15\":null}"
$CLICKHOUSE_CLIENT -q "SELECT a14, a15 FROM t_chain ORDER BY a15"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_chain"

# Two defaults which share their inputs: each not inserted column is defined once, not again in the definition of the other.
COLUMNS="a0 UInt64 DEFAULT 1, b0 UInt64 DEFAULT 1"
for i in $(seq 1 14); do COLUMNS="${COLUMNS}, a${i} UInt64 DEFAULT a$((i - 1)) + b$((i - 1)), b${i} UInt64 DEFAULT a$((i - 1)) + b$((i - 1))"; done
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_shared (${COLUMNS}) ENGINE = Memory"
printf '{"a14":42}\n{"a14":null}\n' > "${DATA_FILE}_shared.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_shared (a14) FROM INFILE '${DATA_FILE}_shared.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_shared (a14) FORMAT JSONEachRow {\"a14\":42} {\"a14\":null}"
$CLICKHOUSE_CLIENT -q "SELECT a14, b14 FROM t_shared ORDER BY a14"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_shared"

# An alias defined in a lambda body hides the not inserted column with the same name. The same as the inline insert: 5, 11.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_lambda_alias (a UInt32 DEFAULT 5, b UInt32 DEFAULT arraySum(arrayMap(x -> (2 AS a) + a + x, [1, 2]))) ENGINE = Memory"
printf '{"b":null}\n' > "${DATA_FILE}_lambda_alias.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_lambda_alias (b) FROM INFILE '${DATA_FILE}_lambda_alias.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_lambda_alias (b) FORMAT JSONEachRow {\"b\":null}"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM t_lambda_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_lambda_alias"

# The lambdas of a default which uses no not inserted column are not changed: "y" is bound to the inner "x", as in the inline insert: 36.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_nested_lambda (b UInt32 DEFAULT arraySum(arrayMap(x -> arraySum(arrayMap(x -> y, [1, 2])) + (x AS y), [10, 20]))) ENGINE = Memory"
printf '{"b":null}\n' > "${DATA_FILE}_nested_lambda.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_nested_lambda (b) FROM INFILE '${DATA_FILE}_nested_lambda.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_nested_lambda (b) FORMAT JSONEachRow {\"b\":null}"
$CLICKHOUSE_CLIENT -q "SELECT b FROM t_nested_lambda"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_nested_lambda"

# The lambda argument "q" hides the alias "q" of the first use of "a", but "a" in the lambda body is still the column. The same as the inline insert: 5, 18.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_hidden_alias (a UInt32 DEFAULT 5, b UInt32 DEFAULT (a AS q) + arraySum(arrayMap(q -> a + q, [1, 2]))) ENGINE = Memory"
printf '{"b":null}\n' > "${DATA_FILE}_hidden_alias.jsonl"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_hidden_alias (b) FROM INFILE '${DATA_FILE}_hidden_alias.jsonl' FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT --input_format_null_as_default 1 -q "INSERT INTO t_hidden_alias (b) FORMAT JSONEachRow {\"b\":null}"
$CLICKHOUSE_CLIENT -q "SELECT a, b FROM t_hidden_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_hidden_alias"

rm -f "${DATA_FILE}.json" "${DATA_FILE}.jsonl" "${DATA_FILE}_c.json" "${DATA_FILE}_lambda.jsonl" "${DATA_FILE}_capture.jsonl" "${DATA_FILE}_alias_capture.jsonl" "${DATA_FILE}_alias.jsonl" "${DATA_FILE}_chain.jsonl" "${DATA_FILE}_shared.jsonl" "${DATA_FILE}_lambda_alias.jsonl" "${DATA_FILE}_nested_lambda.jsonl" "${DATA_FILE}_hidden_alias.jsonl"
