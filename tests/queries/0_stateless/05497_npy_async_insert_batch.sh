#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One async insert batch reuses one Npy input format for all its entries; each entry must be read in full.

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=100000&async_insert_busy_timeout_max_ms=100000"

# insert <table> <declared rows> <data bytes to cut> <values...>: a 1-D '<i8' Npy array.
function insert()
{
    local table=$1
    shift
    python3 -c "
import struct, sys
rows, cut, values = int(sys.argv[1]), int(sys.argv[2]), [int(v) for v in sys.argv[3:]]
header = (\"{'descr': '<i8', 'fortran_order': False, 'shape': (%d,), }\" % rows).encode()
header += b' ' * (63 - (10 + len(header)) % 64) + b'\n'
data = struct.pack('<%dq' % len(values), *values)
sys.stdout.buffer.write(b'\x93NUMPY\x01\x00' + struct.pack('<H', len(header)) + header + data[:len(data) - cut])
" "$@" | ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+${table}+FORMAT+Npy" --data-binary @-
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_valid (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_after_error (x Int64) ENGINE = MergeTree ORDER BY x;
"

# Entries of 2, 3 and 1 rows.
insert t_valid 2 0 1 2
insert t_valid 3 0 3 4 5
insert t_valid 1 0 6
# An entry that declares 3 rows but ends inside its 2nd row, then a valid entry.
insert t_after_error 3 12 3 4 5
insert t_after_error 2 0 1 2

for table in t_valid t_after_error; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${table}"
    ${CLICKHOUSE_CLIENT} -q "SELECT groupArray(x) FROM (SELECT x FROM ${table} ORDER BY x)"
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_valid; DROP TABLE t_after_error"
