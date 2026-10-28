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

rm -f "${DATA_FILE}.json" "${DATA_FILE}.jsonl" "${DATA_FILE}_c.json"
