#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
trap 'rm -f "$FILE"' EXIT

# Prints the number of rows that are not pruned, and the result with and without the filter pushdown.
function check()
{
    local condition=$1
    echo "$condition"
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$FILE', 'ORC', 'number UInt64, t DateTime64(9, \'UTC\')') WHERE indexHint($condition);
        SELECT count(), min(number), max(number) FROM file('$FILE', 'ORC', 'number UInt64, t DateTime64(9, \'UTC\')') WHERE $condition;
        SELECT count(), min(number), max(number) FROM file('$FILE', 'ORC', 'number UInt64, t DateTime64(9, \'UTC\')') WHERE $condition
            SETTINGS input_format_orc_filter_push_down = 0"
}

# The timestamps before the epoch with a negative sub-millisecond part, from -1.000000001 to -0.001000001 seconds,
# in row groups of 100 rows. Their statistics must allow the pruning.
$CLICKHOUSE_LOCAL -q "
    SELECT number, fromUnixTimestamp64Nano(toInt64(number) * 1000000 - 1000000001, 'UTC') AS t FROM numbers(1000)
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_row_index_stride = 100"
check "t < fromUnixTimestamp64Nano(-500000001, 'UTC')"
check "t <= fromUnixTimestamp64Nano(-500000001, 'UTC')"
check "t = fromUnixTimestamp64Nano(-950000001, 'UTC')"

# A timestamp in the millisecond after -1 second, in its own row group: the pruning must keep it.
$CLICKHOUSE_LOCAL -q "
    SELECT number, fromUnixTimestamp64Nano([-2000000000, -999500000, -500000000][number + 1], 'UTC') AS t FROM numbers(3)
    INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_row_index_stride = 1"
check "t = fromUnixTimestamp64Nano(-999500000, 'UTC')"

# A file from PyArrow (the C++ writer of ORC) with a bloom filter, which rounds the negative milliseconds toward zero,
# and one row in each row group: the bloom filter must keep the matching rows.
for value in -2000000000 -1 -999500000 -1500000 1000000000; do
    $CLICKHOUSE_LOCAL -q "
        SELECT $value, count() FROM file('$CUR_DIR/data_orc/timestamps_before_epoch_pyarrow_bloom_filter.orc', 'ORC', 't Nullable(DateTime64(9))')
        WHERE t = fromUnixTimestamp64Nano($value)"
done
