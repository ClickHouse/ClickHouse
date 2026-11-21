#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}

# The number of axes of the row space is not bounded by the amount of data: every repeated use of
# a dimension is an axis of its own, and a dimension of length one adds neither rows nor bytes. The
# order of the axes was found with a scan of all the axes for every axis, so a file of one row and
# a couple of megabytes took hundreds of billions of steps. Two files pin that the ordering is not
# quadratic: one with 256 variables over 1024 uses of their own dimension each (262144 axes whose
# orders agree), and one where 50000 variables over pairs of dimensions form a cycle, so the order
# is contradictory and the axes are also taken out of turn.
python3 - "$PREFIX" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_INT = 10, 11, 4
ABSENT = struct.pack('>ii', 0, 0)


def tag(value):
    return struct.pack('>i', value)


def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)


def build(num_dimensions, variables):
    """Every dimension has the length one; variables: [(name, [dimension ids], value)]"""
    def header(first_begin):
        parts = [b'CDF\x01', tag(0), tag(NC_DIMENSION), tag(num_dimensions)]
        for i in range(num_dimensions):
            parts.append(name('d%d' % i) + tag(1))
        parts.append(ABSENT)
        parts.append(tag(NC_VARIABLE) + tag(len(variables)))
        for i, (variable_name, dimension_ids, _) in enumerate(variables):
            parts.append(name(variable_name) + tag(len(dimension_ids)))
            parts.append(struct.pack('>%di' % len(dimension_ids), *dimension_ids))
            parts.append(ABSENT + tag(NC_INT) + tag(4) + tag(first_begin + 4 * i))
        return b''.join(parts)

    return header(len(header(0))) + b''.join(tag(value) for _, _, value in variables)


prefix = sys.argv[1]

with open(prefix + '_repeated.nc', 'wb') as out:
    out.write(build(256, [('v%d' % i, [i] * 1024, 1000 + i) for i in range(256)]))

num_cycle = 50000
with open(prefix + '_cycle.nc', 'wb') as out:
    out.write(build(num_cycle, [('v%d' % i, [i, (i + 1) % num_cycle], 1000 + i) for i in range(num_cycle)]))
PYTHON

echo '--- repeated'
$CLICKHOUSE_LOCAL -q "SELECT v0, v255 FROM file('${PREFIX}_repeated.nc', NetCDF, 'v0 Int32, v255 Int32')"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${PREFIX}_repeated.nc', NetCDF)" | wc -l
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${PREFIX}_repeated.nc', NetCDF)"

echo '--- cycle'
$CLICKHOUSE_LOCAL -q "SELECT v0, v49999 FROM file('${PREFIX}_cycle.nc', NetCDF, 'v0 Int32, v49999 Int32')"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${PREFIX}_cycle.nc', NetCDF, 'v0 Int32')"

rm -f "${PREFIX}_repeated.nc" "${PREFIX}_cycle.nc"
