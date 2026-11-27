#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_repeated_unit.nc

# Every repeated use of a dimension is an axis of its own, and with
# `input_format_netcdf_add_dimension_columns` every such axis gets a column with a name of its own:
# `d`, `d_index`, `d_index_2` and so on. The search for a free name started over for every axis, so
# a file of one row with a variable that uses a dimension of length one 65536 times took billions
# of probes. The variables `d_index_3` and `d_index_65535` take two of the names, so the generated
# columns have to skip them.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_INT = 10, 11, 4
ABSENT = struct.pack('>ii', 0, 0)


def tag(value):
    return struct.pack('>i', value)


def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)


variables = [('v', [0] * 65536, 1000), ('d_index_3', [], 3), ('d_index_65535', [], 65535)]


def header(first_begin):
    parts = [b'CDF\x01', tag(0), tag(NC_DIMENSION), tag(1), name('d') + tag(1), ABSENT]
    parts.append(tag(NC_VARIABLE) + tag(len(variables)))
    for i, (variable_name, dimension_ids, _) in enumerate(variables):
        parts.append(name(variable_name) + tag(len(dimension_ids)))
        parts.append(struct.pack('>%di' % len(dimension_ids), *dimension_ids))
        parts.append(ABSENT + tag(NC_INT) + tag(4) + tag(first_begin + 4 * i))
    return b''.join(parts)


with open(sys.argv[1], 'wb') as out:
    out.write(header(len(header(0))) + b''.join(tag(value) for _, _, value in variables))
PYTHON

SETTINGS="SETTINGS input_format_netcdf_add_dimension_columns = 1"

echo '--- the data'
$CLICKHOUSE_LOCAL -q "SELECT v FROM file('$FILE', NetCDF, 'v Int32') $SETTINGS"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF) $SETTINGS"

DESCRIPTION=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_repeated_unit.tsv
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE', NetCDF) $SETTINGS" | cut -f 1,2 > "$DESCRIPTION"

echo '--- the number of columns and of distinct names'
wc -l < "$DESCRIPTION"
cut -f 1 "$DESCRIPTION" | sort -u | wc -l

echo '--- the first and the last names'
head -n 5 "$DESCRIPTION"
tail -n 8 "$DESCRIPTION"

echo '--- the columns of the axes and the variables that took their names'
$CLICKHOUSE_LOCAL -q "SELECT d, d_index, d_index_2, d_index_3, d_index_4, d_index_65535, d_index_65537
    FROM file('$FILE', NetCDF, 'd UInt64, d_index UInt64, d_index_2 UInt64, d_index_3 Int32, d_index_4 UInt64, d_index_65535 Int32, d_index_65537 UInt64') $SETTINGS"

rm -f "$FILE" "$DESCRIPTION"
