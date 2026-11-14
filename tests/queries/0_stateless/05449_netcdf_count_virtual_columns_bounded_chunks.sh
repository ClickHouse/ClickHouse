#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.nc
rm -f "$FILE"

# `int a(x)` and `int b(y)` with 4096 elements each span a row space of 16777216 rows in a file of 32 KiB.
# A query of only virtual columns does not read the data, but the virtual columns are materialized for
# every row of a chunk, so the chunks of such a count must stay bounded to let `LIMIT 1` stop the query.
python3 - "$FILE" <<'PYTHON'
import struct
import sys

NC_DIMENSION, NC_VARIABLE, NC_INT = 10, 11, 4
ABSENT = struct.pack('>ii', 0, 0)
X = Y = 4096

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

def make_header(a_begin, b_begin):
    header = b'CDF\x01' + tag(0)
    header += tag(NC_DIMENSION) + tag(2) + name('x') + tag(X) + name('y') + tag(Y) + ABSENT
    header += tag(NC_VARIABLE) + tag(2)
    header += name('a') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(4 * X) + tag(a_begin)
    header += name('b') + tag(1) + tag(1) + ABSENT + tag(NC_INT) + tag(4 * Y) + tag(b_begin)
    return header

size = len(make_header(0, 0))
with open(sys.argv[1], 'wb') as out:
    out.write(make_header(size, size + 4 * X))
    out.write(b''.join(struct.pack('>i', i) for i in range(X)))
    out.write(b''.join(struct.pack('>i', i) for i in range(Y)))
PYTHON

SETTINGS="use_cache_for_count_from_files = 0, optimize_count_from_files = 1, max_threads = 1, max_memory_usage = '32Mi'"

$CLICKHOUSE_LOCAL -q "SELECT _size > 0 FROM file('$FILE', NetCDF, 'a Int32, b Int32') LIMIT 1 SETTINGS $SETTINGS"
$CLICKHOUSE_LOCAL -q "SELECT max(blockSize()) <= 65536 FROM (SELECT _file FROM file('$FILE', NetCDF, 'a Int32, b Int32') LIMIT 100000) SETTINGS $SETTINGS"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', NetCDF, 'a Int32, b Int32') SETTINGS $SETTINGS"

rm -f "$FILE"
