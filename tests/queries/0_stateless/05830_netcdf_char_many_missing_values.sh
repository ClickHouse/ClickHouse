#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A `char` variable that is read as one character per row can have a `missing_value` attribute of
# millions of characters. Its sentinels are prepared once per variable, so a value is not compared
# to every character of the attribute, which took `rows * attribute bytes` comparisons.
# CDF-1 `int v(x)` makes `x` an axis of the row space, so `char qc(x)` is one character per row,
# cycling through 'a', 'b', 'c', 'd'. Its `missing_value` is 1000000 times 'b' followed by 'c'.
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
NC_CHAR, NC_INT = 2, 4
ABSENT = tag(0) + tag(0)
N = 65536

sentinels = b'b' * 1000000 + b'c'

def make_header(begin):
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(N)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('v') + tag(1) + tag(0) + ABSENT
    header += tag(NC_INT) + tag(4 * N) + tag(begin)
    header += name('qc') + tag(1) + tag(0) + tag(NC_ATTRIBUTE) + tag(1) + attribute('missing_value', NC_CHAR, len(sentinels), sentinels)
    header += tag(NC_CHAR) + tag(N) + tag(begin + 4 * N)
    return header

header = make_header(len(make_header(0)))

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    out.write(struct.pack('>%di' % N, *range(N)))
    out.write(bytes(b'abcd'[i % 4] for i in range(N)))
PYTHON

echo "--- without the setting, the sentinels are values"
$CLICKHOUSE_LOCAL -q "SELECT count(), countIf(qc = 'b'), countIf(qc = 'c') FROM file('$FILE', NetCDF)"

echo "--- with the setting, every sentinel is NULL"
$CLICKHOUSE_LOCAL -q "
    SELECT toTypeName(qc), count(), countIf(qc IS NULL), arraySort(groupUniqArray(qc))
    FROM file('$FILE', NetCDF)
    SETTINGS input_format_netcdf_fill_value_as_null = 1, max_threads = 1"

echo "--- the first row does not wait for comparisons of every value with every sentinel"
$CLICKHOUSE_LOCAL -q "SELECT v, qc FROM file('$FILE', NetCDF) LIMIT 1 SETTINGS input_format_netcdf_fill_value_as_null = 1, max_threads = 1, max_execution_time = 60"

rm -f "$FILE"
