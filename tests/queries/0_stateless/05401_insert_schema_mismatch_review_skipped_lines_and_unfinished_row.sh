#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The structure-mismatch explanation must not be derived from rows the parser never reached:
# lines skipped before the header and the header row itself must not shift the row bound under
# parallel parsing (neither for the inline/stdin path nor for `FROM INFILE`, which takes the bound
# from the "(at row N)" part of the message), and a streamed capture that ends inside a row must not
# be sampled as if the row was complete.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

PARALLEL='--input_format_parallel_parsing 1 --max_insert_block_size 3 --max_block_size 3 --min_chunk_bytes_for_parallel_parsing 1 --max_threads 4'
SERIAL='--input_format_parallel_parsing 0 --max_insert_block_size 3 --max_block_size 3'

run() {
    printf "$1" | $CLICKHOUSE_LOCAL $4 --query "CREATE TABLE t ($2) ENGINE = Memory; INSERT INTO t FORMAT $3" 2>&1 | check
}

echo "-- TSV with skipped first lines"
for mode in "$PARALLEL" "$SERIAL"; do
    run 'ignored\n1\t2\n1\t2\n1\t1.5\n2\ttext\n' 'a UInt8, b UInt8' TSV \
        "$mode --input_format_tsv_skip_first_lines 1 --input_format_tsv_detect_header 0"
done
run 'ignored\n1\t2\n1\t2\ntext\t1\n2\ttext\n' 'a UInt8, b UInt8' TSV \
    "$PARALLEL --input_format_tsv_skip_first_lines 1 --input_format_tsv_detect_header 0"

echo "-- TSVWithNames from INFILE"
DATA_FILE="$CLICKHOUSE_TMP/05401_data_$CLICKHOUSE_DATABASE.tsv"
infile() {
    printf "$1" > "$DATA_FILE"
    $CLICKHOUSE_LOCAL $2 --query "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FROM INFILE '$DATA_FILE' FORMAT TSVWithNames" 2>&1 | check
}
for mode in "$PARALLEL" "$SERIAL"; do
    infile 'a\tb\n1\t2\n1\t2\n1\t1.5\n2\ttext\n' "$mode"
done
infile 'a\tb\n1\t2\n1\t2\ntext\t1\n2\ttext\n' "$PARALLEL"

echo "-- A streamed row longer than the first captured buffer"
{ printf '1.5\t'; head -c 2097152 /dev/zero | tr '\0' 'a'; printf '\t1\n'; } > "$DATA_FILE"
$CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 --input_format_tsv_detect_header 0 --query "
    CREATE TABLE t (n UInt8, s String, k UInt8) ENGINE = Memory;
    INSERT INTO t FORMAT TSV
" < "$DATA_FILE" 2>&1 | check
printf 'text\thello\n' | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 --query "
    CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory;
    INSERT INTO t FORMAT TSV
" 2>&1 | check

rm -f "$DATA_FILE"
