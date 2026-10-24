#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CANNOT_PARSE_TEXT` error thrown from the batched evaluation of templated expressions on the
# `INSERT ... FROM INFILE FORMAT Values` path must carry the row bound (` in one of the first N rows`),
# like parse errors do, so that schema inference for the structure-mismatch explanation samples only
# the rows the parser has read. Appending a row from the next block must not change the explanation.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    echo "$out" | grep -o "Code: [0-9]*" | head -n1
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.values"

run() {
    $CLICKHOUSE_LOCAL --max_block_size 2 --max_insert_block_size 2 --input_format_parallel_parsing 0 --query "
        CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory;
        INSERT INTO t FROM INFILE '${DATA_FILE}' FORMAT Values;
    " < /dev/null 2>&1 | check
}

echo "-- two rows"
printf "(1,'oops'),(2,'oops')" > "$DATA_FILE"
run

echo "-- a third row from the next block is not sampled"
printf "(1,'oops'),(2,'oops'),(3,1)" > "$DATA_FILE"
run

rm -f "$DATA_FILE"
