#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# A C writer that passes `sizeof("true")` to `nc_put_att_text` stores the terminating zero byte in
# the `_Unsigned` attribute. The reference decoder `netCDF4` removes the zero bytes of the text, so
# `true\0` and `True\0` make the variable unsigned as well.
# CDF-1 `short v(x)` with `_Unsigned = "true\0"` and `_FillValue` (raw -2), `short w(x)` with
# `_Unsigned = "True\0"`, and `short s(x)` without the attribute, all with the raw values [-1, 7, -2].
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
NC_CHAR, NC_SHORT = 2, 3
ABSENT = tag(0) + tag(0)

def make_header(begin):
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3)
    header += ABSENT
    header += tag(NC_VARIABLE) + tag(3)
    attributes = [attribute('_Unsigned', NC_CHAR, 5, b'true\x00'), attribute('_FillValue', NC_SHORT, 1, struct.pack('>h', -2))]
    header += name('v') + tag(1) + tag(0) + tag(NC_ATTRIBUTE) + tag(len(attributes)) + b''.join(attributes)
    header += tag(NC_SHORT) + tag(8) + tag(begin)
    attributes = [attribute('_Unsigned', NC_CHAR, 5, b'True\x00')]
    header += name('w') + tag(1) + tag(0) + tag(NC_ATTRIBUTE) + tag(len(attributes)) + b''.join(attributes)
    header += tag(NC_SHORT) + tag(8) + tag(begin + 8)
    header += name('s') + tag(1) + tag(0) + ABSENT + tag(NC_SHORT) + tag(8) + tag(begin + 16)
    return header

header = make_header(len(make_header(0)))

with open(sys.argv[1], 'wb') as out:
    out.write(header)
    for _ in range(3):
        out.write(struct.pack('>hhh', -1, 7, -2) + b'\x00\x00')
PYTHON

echo "--- a NUL-terminated _Unsigned attribute makes the variable unsigned"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF)" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT v, w, s FROM file('$FILE', NetCDF)"

echo "--- and its _FillValue is read as NULL"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1" | cut -f1,2
$CLICKHOUSE_LOCAL -q "SELECT v, w, s FROM file('$FILE', NetCDF) SETTINGS input_format_netcdf_fill_value_as_null = 1"

rm -f "$FILE"
