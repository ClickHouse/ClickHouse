#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FORMATS=('TSVWithNames' 'CSVWithNames')
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parsing_with_names"

for format in "${FORMATS[@]}"
do
    # Columns are permuted
    $CLICKHOUSE_CLIENT -q "CREATE TABLE parsing_with_names(c FixedString(16), a DateTime('Asia/Dubai'),  b String) ENGINE=Memory()"

    echo "$format, false";
    $CLICKHOUSE_CLIENT --max_threads=0 --max_block_size=65505 --output_format_parallel_formatting=false -q \
    "SELECT URLRegions as d, toTimeZone(ClientEventTime, 'Asia/Dubai') as a, MobilePhoneModel as b, ParamPrice as e, ClientIP6 as c FROM test.hits LIMIT 50000 Format $format" | \
    $CLICKHOUSE_CLIENT --max_threads=0 --max_block_size=65505 --input_format_skip_unknown_fields=1 --input_format_parallel_parsing=false -q "INSERT INTO parsing_with_names FORMAT $format"

    $CLICKHOUSE_CLIENT -q "SELECT * FROM parsing_with_names;" | md5sum
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parsing_with_names"


    $CLICKHOUSE_CLIENT -q "CREATE TABLE parsing_with_names(c FixedString(16), a DateTime('Asia/Dubai'),  b String) ENGINE=Memory()"
    echo "$format, true";
    $CLICKHOUSE_CLIENT --max_threads=0 --max_block_size=65505 --output_format_parallel_formatting=false -q \
    "SELECT URLRegions as d, toTimeZone(ClientEventTime, 'Asia/Dubai') as a, MobilePhoneModel as b, ParamPrice as e, ClientIP6 as c FROM test.hits LIMIT 50000 Format $format" | \
    $CLICKHOUSE_CLIENT --max_threads=0 --max_block_size=65505 --input_format_skip_unknown_fields=1 --input_format_parallel_parsing=true -q "INSERT INTO parsing_with_names FORMAT $format"

    $CLICKHOUSE_CLIENT -q "SELECT * FROM parsing_with_names;" | md5sum
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS parsing_with_names"
done
