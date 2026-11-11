#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# On the `INSERT ... FROM INFILE` path the number of rows the parser reached is taken from the parse
# error message. `IRowInputFormat` appends its `(at row N)` marker and then a verbose diagnostic that
# prints the fields of the failing row that were parsed successfully. A fake marker inside such a field
# therefore ends up after the genuine one and must not extend the range sampled by schema inference.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/05616_spoofed_marker_${CLICKHOUSE_DATABASE}.tsv"
DATA_FILE_MISMATCH="${CLICKHOUSE_TMP}/05616_spoofed_marker_mismatch_${CLICKHOUSE_DATABASE}.tsv"

# The first field is parsed successfully and contains a fake `(at row 50)` marker, the second one does
# not fit the destination. The second row would change the inferred type of the second column if it
# were sampled.
printf 'hello (at row 50)\t1.5\nworld\ttext\n' > "$DATA_FILE"
# The same fake marker, but here the failing row itself really does not match the destination.
printf 'hello (at row 50)\ttext\nworld\ttext\n' > "$DATA_FILE_MISMATCH"

echo "-- the fake marker does not extend the sampled range (no false positive)"
$CLICKHOUSE_LOCAL --input_format_tsv_detect_header 0 --query "
    CREATE TABLE test_spoofed_marker (s String, n UInt8) ENGINE = Memory;
    INSERT INTO test_spoofed_marker FROM INFILE '${DATA_FILE}' FORMAT TSV;
" < /dev/null 2>&1 | check

echo "-- the failing row itself does not match the destination (explanation still fires)"
$CLICKHOUSE_LOCAL --input_format_tsv_detect_header 0 --query "
    CREATE TABLE test_spoofed_marker (s String, n UInt8) ENGINE = Memory;
    INSERT INTO test_spoofed_marker FROM INFILE '${DATA_FILE_MISMATCH}' FORMAT TSV;
" < /dev/null 2>&1 | check

rm -f "$DATA_FILE" "$DATA_FILE_MISMATCH"
