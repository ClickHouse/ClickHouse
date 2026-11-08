#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro is not available in the fast test build

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro

# Files from apache/arrow-testing, written by another implementation.
for file in alltypes_plain alltypes_plain.bzip2 alltypes_plain.xz
do
    echo "$file"
    $CLICKHOUSE_LOCAL --session_timezone UTC --input-format Avro --output-format TSV -q "SELECT * FROM table ORDER BY id" < "$DATA_DIR/$file.avro"
done

# Round trip, with several blocks.
for codec in bzip2 xz
do
    echo "$codec"
    $CLICKHOUSE_LOCAL --output_format_avro_codec "$codec" --output_format_avro_sync_interval 1000 \
        -q "SELECT number AS x, toString(number) AS s FROM numbers(10000) FORMAT Avro" > "$CLICKHOUSE_TMP/$codec.avro"
    grep -a -o -F "$codec" "$CLICKHOUSE_TMP/$codec.avro" | head -n1
    $CLICKHOUSE_LOCAL --input-format Avro -q "SELECT count(), sum(x), sum(toUInt64(s)) FROM table" < "$CLICKHOUSE_TMP/$codec.avro"
done
