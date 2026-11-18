#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary threshold that does not fit in `size_t` when multiplied by the number of values.
for threshold in 1e308 -1e308; do
    $CLICKHOUSE_LOCAL -q "SELECT toString(number % 3) AS s FROM numbers(10) FORMAT ORC SETTINGS output_format_orc_dictionary_key_size_threshold = $threshold" \
        | $CLICKHOUSE_LOCAL --input-format ORC -q "SELECT '$threshold', groupArray(s) FROM table"
done

# The extreme `DateTime64` values that cannot be stored in ORC with their statistics in milliseconds.
for value in 'reinterpret(toInt64(9223372036854775807), '"'"'DateTime64(0)'"'"')' \
             'reinterpret(toInt64(-9223372036854775808), '"'"'DateTime64(0)'"'"')' \
             'reinterpret(toInt64(9223372036854775807), '"'"'DateTime64(1)'"'"')'; do
    $CLICKHOUSE_LOCAL -q "SELECT $value AS t FORMAT ORC" 2>&1 >/dev/null | grep -oE '\(VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE\)' | head -n1 | grep . || echo "$value: no error"
done

# The extreme values with the nanoseconds can be stored.
for value in 9223372036854775807 -9223372036854775808; do
    $CLICKHOUSE_LOCAL -q "SELECT reinterpret(toInt64($value), 'DateTime64(9, \'UTC\')') AS t FORMAT ORC" \
        | $CLICKHOUSE_LOCAL --input-format ORC -q "SELECT reinterpret(t, 'Int64') FROM table"
done
