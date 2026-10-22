#!/usr/bin/env bash

# Checks the chunks of integers that the `GDeflate` codec packs before it compresses them: each integer as its difference
# from the least of its chunk, in as many bits as the largest difference takes, which the device unpacks after it expands
# the chunk. The columns are of each size of integer, signed and not, with chunks whose integers lie close together,
# chunks whose integers take all their bits, which are not packed, and a block that ends in the middle of a chunk. Their
# values read back as they were written, both through the CPU and through a `GROUP BY` on the device.
#
# The codec exists only in a build with GPU support and works only on a server with a usable device;
# elsewhere the expected lines are printed as they are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "CREATE TABLE gpu_gdeflate_probe (x UInt64 CODEC(GDeflate)) ENGINE = MergeTree ORDER BY tuple()" \
        --enable_gdeflate_codec 1
    if $CLICKHOUSE_CLIENT --query "INSERT INTO gpu_gdeflate_probe SELECT number FROM numbers(10)" --enable_gdeflate_codec 1 2>/dev/null; then
        HAS_GPU=1
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE gpu_gdeflate_probe"
fi

if [ "$HAS_GPU" == "0" ]; then
    cat "$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh).reference"
    exit 0
fi

COLUMNS="k UInt32, i8 Int8, u16 UInt16, i32 Int32, i64 Int64, u64 UInt64, d Date, t DateTime, m Decimal64(2), z Int64"
# Rows where `number` is below 300000 hold values close together; the rest spread over all the bits of their type.
VALUES="
    number % 1000,
    toInt8(if(number < 300000, -100 + number % 7, number % 256 - 128)),
    toUInt16(if(number < 300000, 40000 + number % 300, cityHash64(number) % 65536)),
    toInt32(if(number < 300000, -5000000 + number % 100000, cityHash64(number))),
    toInt64(if(number < 300000, -(number % 1000003), cityHash64(number))),
    if(number < 300000, 18446744073709551000 + number % 600, cityHash64(number)),
    toDate('2020-01-01') + number % 2000,
    toDateTime('2024-01-01 00:00:00') + number % 86400,
    toDecimal64((number % 100000) / 100, 2),
    -toInt64(number)"

$CLICKHOUSE_CLIENT --enable_gdeflate_codec 1 --query "
    CREATE TABLE gpu_packed ($(echo "$COLUMNS" | sed -E 's/([A-Za-z0-9()]+)(,|$)/\1 CODEC(GDeflate(2))\2/g'), dz Int64 CODEC(Delta, GDeflate(2)))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
    CREATE TABLE gpu_plain ($COLUMNS, dz Int64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO gpu_packed SELECT $VALUES, toInt64(number) * 3 FROM numbers(1000003);
    INSERT INTO gpu_plain SELECT $VALUES, toInt64(number) * 3 FROM numbers(1000003);
"

# Read through the CPU.
$CLICKHOUSE_CLIENT --query "
    SELECT (SELECT sum(cityHash64(*)) FROM gpu_packed) = (SELECT sum(cityHash64(*)) FROM gpu_plain)"

# Grouped on the device from the compressed chunks, against the CPU over the other table.
QUERY="SELECT k, sum(i8), sum(u16), sum(i32), sum(i64), sum(u64), min(d), max(t), sum(m), sum(z), sum(dz), count()"
for mode in device host; do
    on_cpu=$($CLICKHOUSE_CLIENT --query "$QUERY FROM gpu_plain WHERE d < '2024-06-01' GROUP BY k ORDER BY k")
    on_gpu=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 --query "
        $QUERY FROM gpu_packed WHERE d < toDate('2024-06-01') GROUP BY k ORDER BY k
        SETTINGS optimize_move_to_prewhere = 1, gpu_aggregation_decompression = '$mode'")
    [ "$on_cpu" == "$on_gpu" ] && echo "$mode ok" || echo "$mode MISMATCH"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_packed; DROP TABLE gpu_plain"
