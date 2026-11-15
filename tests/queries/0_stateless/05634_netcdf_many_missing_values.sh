#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A `missing_value` attribute can hold millions of sentinels. They are prepared once per variable
# for a lookup, so a value is not compared to every sentinel, which used to take `rows * sentinels`
# comparisons and kept the query running far beyond `max_execution_time`.
# CDF-1 `int v(x)` with 65536 values `i % 4 * 500000` (0, 500000, 1000000, 1500000) and
# `missing_value` holding the integers 1 to 1000000; `double d(x)` with the values
# `i % 4 - 1.5` (-1.5, -0.5, 0.5, 1.5) and `missing_value` holding 0.5, 1.5 and the doubles
# 2 to 200001; `double z(x)` with alternating -0.0 and NaN values and `missing_value` [+0.0, NaN].
python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

def attribute(name_, type_, num_elements, values):
    return name(name_) + tag(type_) + tag(num_elements) + values + b'\x00' * ((4 - len(values) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_ATTRIBUTE = 10, 11, 12
NC_INT, NC_DOUBLE = 4, 6
ABSENT = tag(0) + tag(0)
N = 65536

int_sentinels = struct.pack('>%di' % 1000000, *range(1, 1000001))
double_sentinels = struct.pack('>%dd' % 200002, 0.5, 1.5, *[float(i) for i in range(2, 200002)])
zero_sentinels = struct.pack('>dd', 0.0, float('nan'))

def make_header(begin):
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(N)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(3)
    header += name('v') + tag(1) + tag(0) + tag(NC_ATTRIBUTE) + tag(1) + attribute('missing_value', NC_INT, 1000000, int_sentinels)
    header += tag(NC_INT) + tag(4 * N) + tag(begin)
    header += name('d') + tag(1) + tag(0) + tag(NC_ATTRIBUTE) + tag(1) + attribute('missing_value', NC_DOUBLE, 200002, double_sentinels)
    header += tag(NC_DOUBLE) + tag(8 * N) + tag(begin + 4 * N)
    header += name('z') + tag(1) + tag(0) + tag(NC_ATTRIBUTE) + tag(1) + attribute('missing_value', NC_DOUBLE, 2, zero_sentinels)
    header += tag(NC_DOUBLE) + tag(8 * N) + tag(begin + 12 * N)
    return header

header = make_header(len(make_header(0)))

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    out.write(struct.pack('>%di' % N, *[i % 4 * 500000 for i in range(N)]))
    out.write(struct.pack('>%dd' % N, *[i % 4 - 1.5 for i in range(N)]))
    out.write(struct.pack('>%dd' % N, *[-0.0 if i % 2 == 0 else float('nan') for i in range(N)]))
PYTHON

echo "--- without the setting, the sentinels are values"
$CLICKHOUSE_LOCAL -q "SELECT count(), countIf(v = 0), countIf(d = 0.5) FROM file('$FILE', NetCDF)"

echo "--- with the setting, every sentinel is NULL"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), countIf(v IS NULL), countIf(d IS NULL), countIf(z IS NULL), arraySort(groupUniqArray(v)), arraySort(groupUniqArray(d))
    FROM file('$FILE', NetCDF)
    SETTINGS input_format_netcdf_fill_value_as_null = 1, max_threads = 1"

echo "--- the first row does not wait for comparisons of every value with every sentinel"
$CLICKHOUSE_LOCAL -q "SELECT v FROM file('$FILE', NetCDF) LIMIT 1 SETTINGS input_format_netcdf_fill_value_as_null = 1, max_threads = 1, max_execution_time = 60"

rm -f "$FILE"
