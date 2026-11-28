#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Regexp` format splits rows only by `\n`. Parallel parsing must count rows the same way,
# otherwise lone `\r` characters shift the row numbers reported in errors and may split a row.
# https://github.com/ClickHouse/ClickHouse/issues/34203

SETTINGS=(--min_chunk_bytes_for_parallel_parsing 1 --max_threads 4 --format_regexp_escaping_rule Raw --format_regexp '^([^ ]+) ([^ ]+) (\d+)$' --input-format Regexp --structure 'x String, y String, z UInt64')

function gen_cr()
{
    for i in $(seq 1 1000); do
        if [ "$i" -eq 700 ]; then
            [ "$1" = "bad" ] && printf 'garbage\n'
        elif [ $((i % 10)) -eq 0 ]; then
            printf 'a b\r%d %d\n' "$i" "$i"
        else
            printf 'a b%d %d\n' "$i" "$i"
        fi
    done
}

echo "lone CR, error row"
for parallel in 0 1; do
    gen_cr bad | $CLICKHOUSE_LOCAL --input_format_parallel_parsing $parallel "${SETTINGS[@]}" --query "SELECT * FROM table FORMAT Null" 2>&1 | grep -o 'at row [0-9]*'
done

echo "lone CR, data"
for parallel in 0 1; do
    gen_cr good | $CLICKHOUSE_LOCAL --input_format_parallel_parsing $parallel "${SETTINGS[@]}" --query "SELECT count(), sum(z), countIf(position(y, '\r') > 0), sum(cityHash64(x, y, z)) FROM table"
done

echo "CRLF, error row"
for parallel in 0 1; do
    for i in $(seq 1 1000); do
        if [ "$i" -eq 700 ]; then printf 'garbage\r\n'; else printf 'a b %d\r\n' "$i"; fi
    done | $CLICKHOUSE_LOCAL --input_format_parallel_parsing $parallel "${SETTINGS[@]}" --query "SELECT * FROM table FORMAT Null" 2>&1 | grep -o 'at row [0-9]*'
done
