#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# `_Unsigned = "true"` applies to the signed 64-bit type of CDF-5 as well: `int64 v(x)` with the
# raw values [-1, 7, -2] is read as `UInt64`, and its `_FillValue` (raw -2) is compared by bits.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

def tag(value):
    return struct.pack('>i', value)

def size(value):
    return struct.pack('>q', value)

def name(value):
    data = value.encode()
    return size(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

def attribute(name_, type_, num_elements, values):
    return name(name_) + tag(type_) + size(num_elements) + values + b'\x00' * ((4 - len(values) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_ATTRIBUTE = 10, 11, 12
NC_CHAR, NC_INT64 = 2, 10
ABSENT = tag(0) + size(0)

def make_header(begin_unsigned, begin_signed):
    header = b'CDF\x05' + size(0)
    header += tag(NC_DIMENSION) + size(1) + name('x') + size(3)
    header += ABSENT
    header += tag(NC_VARIABLE) + size(2)
    attributes = [attribute('_Unsigned', NC_CHAR, 4, b'true'), attribute('_FillValue', NC_INT64, 1, struct.pack('>q', -2))]
    header += name('v') + size(1) + size(0) + tag(NC_ATTRIBUTE) + size(len(attributes)) + b''.join(attributes)
    header += tag(NC_INT64) + size(24) + size(begin_unsigned)
    header += name('s') + size(1) + size(0) + ABSENT + tag(NC_INT64) + size(24) + size(begin_signed)
    return header

header_size = len(make_header(0, 0))
header = make_header(header_size, header_size + 24)

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    out.write(struct.pack('>qqq', -1, 7, -2))
    out.write(struct.pack('>qqq', -1, 7, -2))
PYTHON

echo "--- an unsigned int64 variable is read as UInt64"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT v, s FROM file('$FILE', NetCDF)"

echo "--- and its _FillValue is read as NULL"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT v, s FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
