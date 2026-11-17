#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

# Column chunk and page statistics with min_value > max_value, like the UInt32 / UInt64 statistics that polars 0.20
# writes in signed order, are ignored instead of failing the query with INCORRECT_DATA: no pruning by them, no min/max
# answered from them, while the valid statistics of other row groups and pages still prune.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT
F="$WORK_DIR/inverted.parquet"

# Two row groups of one page each: {1, 2^63} / {1, 2^31}, then {10, 20}.
$CLICKHOUSE_LOCAL -q "
    SELECT toNullable(x) AS u64, toNullable(y) AS u32
    FROM values('x UInt64, y UInt32', (1, 1), (9223372036854775808, 2147483648), (10, 10), (20, 20))
    SETTINGS output_format_parquet_row_group_size = 2, output_format_parquet_write_page_index = 1, max_threads = 1
    FORMAT Parquet" > "$F"

# Swap min_value / max_value in the row group 0 statistics and column index of both columns.
python3 - "$F" <<'PYEOF'
import struct, sys
buf = bytearray(open(sys.argv[1], "rb").read())

def varint(pos):
    r = s = 0
    while True:
        b = buf[pos]; pos += 1; r |= (b & 0x7F) << s; s += 7
        if not b & 0x80: return r, pos

def walk(pos, ct, path, out):
    """Skips one Thrift compact protocol value of type `ct`; out[path] = (value start, end, int value)."""
    start, val = pos, None
    if ct == 3: pos += 1
    elif ct in (4, 5, 6): u, pos = varint(pos); val = (u >> 1) ^ -(u & 1)
    elif ct == 7: pos += 8
    elif ct == 8: n, pos = varint(pos); pos += n
    elif ct in (9, 10):
        h = buf[pos]; pos += 1; n = h >> 4
        if n == 15: n, pos = varint(pos)
        for i in range(n): pos = pos + 1 if h & 15 in (1, 2) else walk(pos, h & 15, path + (i,), out)
    elif ct == 12:
        fid = 0
        while buf[pos]:
            h = buf[pos]; pos += 1
            if h >> 4: fid += h >> 4
            else: u, pos = varint(pos); fid = (u >> 1) ^ -(u & 1)
            pos = walk(pos, h & 15, path + (fid,), out)
        pos += 1
    elif ct not in (1, 2): raise ValueError(ct)
    out[path] = (start, pos, val)
    return pos

def swap(a, b):
    (s1, e1, _), (s2, e2, _) = a, b
    assert e1 - s1 == e2 - s2 and buf[s1:e1] != buf[s2:e2], (a, b)
    buf[s1:e1], buf[s2:e2] = bytes(buf[s2:e2]), bytes(buf[s1:e1])

meta = {}
walk(len(buf) - 8 - struct.unpack("<I", buf[-8:-4])[0], 12, (), meta)
assert (4, 1) in meta and (4, 2) not in meta
for c in (0, 1):
    # FileMetaData.row_groups[0].columns[c]: meta_data 3 -> statistics 12 (max 1, min 2, max_value 5, min_value 6).
    st = (4, 0, 1, c, 3, 12)
    assert st + (1,) not in meta and st + (2,) not in meta
    swap(meta[st + (6,)], meta[st + (5,)])
    # ColumnChunk.column_index_offset 6 -> ColumnIndex: min_values 2, max_values 3.
    index = {}
    walk(meta[(4, 0, 1, c, 6)][2], 12, (), index)
    assert (2, 1) not in index
    swap(index[(2, 0)], index[(3, 0)])
open(sys.argv[1], "wb").write(buf)
PYEOF

# The statistics of row group 0, as raw INT64: min_value is 2^63, max_value is 1.
$CLICKHOUSE_LOCAL -q "SELECT row_groups[1].columns[1].statistics.min, row_groups[1].columns[1].statistics.max FROM file('$F', ParquetMetadata)"

RG="input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 0, max_threads = 1"
PAGE="input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 1, max_threads = 1"
for c in u64 u32; do
    $CLICKHOUSE_LOCAL -q "SELECT $c FROM file('$F') WHERE $c > 100 SETTINGS $RG; SELECT sum(value) FROM system.events WHERE event = 'ParquetPrunedRowGroups'"
    $CLICKHOUSE_LOCAL -q "SELECT $c FROM file('$F') WHERE $c > 100 SETTINGS $PAGE; SELECT sum(value) FROM system.events WHERE event = 'ParquetPrunedPages'"
done

TOPK="use_top_k_dynamic_filtering = 1, max_threads = 1"
$CLICKHOUSE_LOCAL -q "SELECT u64 FROM file('$F') ORDER BY u64 DESC LIMIT 1 SETTINGS $TOPK"
$CLICKHOUSE_LOCAL -q "SELECT u64 FROM file('$F') ORDER BY u64 LIMIT 1 SETTINGS $TOPK"

$CLICKHOUSE_LOCAL -q "SELECT min(u64), max(u64), min(u32), max(u32) FROM file('$F') SETTINGS optimize_min_max_from_files = 1"
