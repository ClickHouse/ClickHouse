#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Arrow ORC adapter stores negative nanoseconds for the timestamps before the epoch,
# and the reader must decode them as the reference implementation does.
$CLICKHOUSE_LOCAL -q "SELECT t, toUnixTimestamp64Micro(t) FROM file('$CUR_DIR/data_orc/timestamps_before_epoch_arrow.orc', ORC)"

DATA_FILE="$CLICKHOUSE_TMP/05447_${CLICKHOUSE_DATABASE}.orc"

# Dictionary encoding of strings (with the row index and several row groups), a non-GMT writer time
# zone, and the values that need all integer encodings: repeats, deltas, outliers.
QUERY="SELECT number AS n,
    toString(number % 13) AS low_cardinality,
    if(number % 5 = 0, NULL, concat('value_', toString(number))) AS high_cardinality,
    toDateTime64(1700000000 + number * 37, 3, 'UTC') AS ts,
    if(number % 1000 = 0, 1000000000000, number % 7) AS outliers,
    intDiv(number, 100) AS runs,
    toDecimal128(number, 10) * -3 AS dec
FROM numbers(25000)"

for compression in none zstd lz4 snappy zlib
do
    $CLICKHOUSE_LOCAL -q "$QUERY FORMAT ORC SETTINGS output_format_orc_compression_method = '$compression',
        output_format_orc_dictionary_key_size_threshold = 0.5, output_format_orc_row_index_stride = 1000,
        output_format_orc_writer_time_zone_name = 'America/Los_Angeles'" > "$DATA_FILE"
    $CLICKHOUSE_LOCAL -q "SELECT '$compression', count(), sum(cityHash64(*)) FROM file('$DATA_FILE', ORC,
        'n UInt64, low_cardinality String, high_cardinality Nullable(String), ts DateTime64(3, \'UTC\'), outliers UInt64, runs UInt64, dec Decimal(38, 10)')
        SETTINGS input_format_orc_reader_time_zone_name = 'America/Los_Angeles'"
done
$CLICKHOUSE_LOCAL -q "SELECT 'expected', count(), sum(cityHash64(*)) FROM ($QUERY)"

# Filter pushdown skips the row groups by the statistics in the row index.
$CLICKHOUSE_LOCAL -q "SELECT count(), min(n), max(n) FROM file('$DATA_FILE', ORC) WHERE n BETWEEN 4500 AND 5500 SETTINGS input_format_orc_filter_push_down = 1"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', ORC) WHERE low_cardinality = '12' SETTINGS input_format_orc_filter_push_down = 1"

rm "$DATA_FILE"
