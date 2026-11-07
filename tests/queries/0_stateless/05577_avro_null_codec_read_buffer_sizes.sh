#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

# The payload of a block of the null codec is decoded from the buffer of the input, which cuts it at places that have nothing to do
# with the rows: inside a string, an array or a number. The same files must read the same, whatever the size of the buffer is.
# `storage_file_read_method = 'pread'` is what makes `max_read_buffer_size` slice the file: `clickhouse-local` maps the file whole by default.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

STRUCTURE="id UInt64, s String, a Array(UInt32), n Nullable(Int64), f Float64"
ROWS=12000
# What each file holds: strings of up to 2000 bytes, and in every 97th row of 20 to 50 KB (more than what is looked ahead to find the end of a value),
# so that rows are far apart in size, and empty arrays and arrays of 39 numbers.
DATA="SELECT number AS id, repeat(char(97 + number % 26), if(number % 97 = 0, toUInt32(20000 + (number * 31) % 30000), toUInt32((number * 7919) % 2000))) AS s, CAST(range(number % 40) AS Array(UInt32)) AS a, if(number % 7 = 0, NULL, toInt64(number) - 1000) AS n, number / 8 AS f FROM numbers($ROWS)"
HASH="sum(cityHash64(id, s, a, n, f))"
EXPECTED=$($CLICKHOUSE_LOCAL -q "SELECT $HASH FROM ($DATA)")

# Blocks of 16 KiB (many of them) and of 8 MiB (a few).
for sync_interval in 16384 8388608
do
    $CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$DIR/data_$sync_interval.avro', Avro, '$STRUCTURE') $DATA SETTINGS output_format_avro_codec = 'null', output_format_avro_sync_interval = $sync_interval"
done

for sync_interval in 16384 8388608
do
    echo "sync interval $sync_interval"
    for buffer_size in 512 4093 100000 262144 300007 1048576 5000011
    do
        SETTINGS="SETTINGS storage_file_read_method = 'pread', max_read_buffer_size = $buffer_size, max_read_buffer_size_local_fs = $buffer_size, max_threads = 1"
        result=$($CLICKHOUSE_LOCAL -q "SELECT count(), $HASH = $EXPECTED FROM file('$DIR/data_$sync_interval.avro', Avro, '$STRUCTURE') $SETTINGS")
        echo "$buffer_size: $result"
        # Only a part of the columns, so that the rest is skipped in the middle of the buffer.
        result=$($CLICKHOUSE_LOCAL -q "SELECT sum(id) = $ROWS * ($ROWS - 1) / 2, sum(length(s)), sum(f) FROM file('$DIR/data_$sync_interval.avro', Avro, 'id UInt64, s String, f Float64') $SETTINGS")
        echo "$buffer_size: $result"
    done
done

rm -rf "$DIR"
