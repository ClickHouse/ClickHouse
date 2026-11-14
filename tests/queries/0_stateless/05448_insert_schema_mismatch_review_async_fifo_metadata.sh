#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Review follow-ups for the `INSERT` structure-mismatch diagnostic: the asynchronous insert queue keeps
# serial parsing, a consumed named pipe is not reopened, and the comparison stays inconclusive where
# the inferred schema does not describe what the parser does.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- async insert with explicit parallel parsing in the query SETTINGS succeeds"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_async; CREATE TABLE t_async (x UInt8) ENGINE = Memory"
printf '2\n' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_async+SETTINGS+async_insert=1,wait_for_async_insert=1,input_format_parallel_parsing=1,max_parsing_threads=2+FORMAT+TSV" --data-binary @-
$CLICKHOUSE_CLIENT --query "SELECT * FROM t_async; DROP TABLE t_async"

echo "-- FROM INFILE of a consumed named pipe returns the parse error"
FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.fifo"
rm -f "$FIFO"
mkfifo "$FIFO"
(printf 'text\t1\n' > "$FIFO") &
timeout 60 $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 \
    --query "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FROM INFILE '$FIFO' FORMAT TSV" 2>&1 \
    | grep -q -F "Cannot parse input" && echo "parse error returned"
wait
rm -f "$FIFO"

echo "-- an empty JSON object is not a string token for a Map column"
printf '{"a":{},"n":1.5}\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check
printf '{"a":{},"n":1}\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Map(String, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" --query "SELECT * FROM t"

echo "-- a NULL is not a string token for an Array or a Bool column"
printf '{"a":null,"n":1.5}\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check
printf '{"a":null,"n":1.5}\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Bool, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check
printf '{"a":null,"n":1}\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" --query "SELECT * FROM t"

echo "-- a genuine string token for an Array column is still explained"
printf '{"a":"x","n":1}\n' | $CLICKHOUSE_LOCAL --query "CREATE TABLE t (a Array(UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check

echo "-- CSV: a Tuple column spans several fields"
printf '1,2,1.5\n' | $CLICKHOUSE_LOCAL --input_format_csv_detect_header 0 --query "CREATE TABLE t (a Tuple(UInt8, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV" 2>&1 | check
printf '1,2,1\n' | $CLICKHOUSE_LOCAL --input_format_csv_detect_header 0 --query "CREATE TABLE t (a Tuple(UInt8, UInt8), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT CSV" --query "SELECT * FROM t"

echo "-- JSON: a meta field not followed by a data array is ordinary data"
printf '{"meta":[{"name":"n","type":"String"}],"data":{},"n":1.5}' | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 \
    --query "CREATE TABLE t (meta String, data String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSON" 2>&1 | check
printf '{"meta":[{"name":"n","type":"String"}],"data":{},"n":1}' | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 \
    --query "CREATE TABLE t (meta String, data String, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSON" --query "SELECT n FROM t"

echo "-- metadata names are validated exactly"
printf '{"meta":[{"name":"A","type":"String"},{"name":"n","type":"UInt8"}],"data":{"A":[1],"n":[1.5]}}' | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 \
    --query "CREATE TABLE t (a UInt8, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONColumnsWithMetadata" 2>&1 | check
printf '{"meta":[{"name":"A","type":"String"},{"name":"n","type":"UInt8"}],"data":{"A":[1],"n":[1]}}' | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 \
    --query "CREATE TABLE t (a UInt8, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONColumnsWithMetadata" --query "SELECT * FROM t"
printf '{"meta":[{"name":"A","type":"String"},{"name":"n","type":"UInt8"}],"data":[{"A":1,"n":1.5}]}' | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 \
    --query "CREATE TABLE t (a UInt8, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSON" 2>&1 | check
printf '{"meta":[{"name":"A","type":"String"},{"name":"n","type":"UInt8"}],"data":[{"A":1,"n":1}]}' | $CLICKHOUSE_LOCAL --input_format_json_validate_types_from_metadata 1 \
    --query "CREATE TABLE t (a UInt8, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSON" --query "SELECT * FROM t"
