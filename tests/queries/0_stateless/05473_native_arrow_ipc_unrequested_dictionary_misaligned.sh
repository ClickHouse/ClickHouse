#!/usr/bin/env bash
# Tags: no-fasttest
# A misaligned vector of 8-byte elements in the `DictionaryBatch` of an unrequested column must not fail a
# projected read: the alignment of a dictionary batch is checked only when that dictionary is decoded.
# Here the `nodes` vector of `b`'s dictionary batch is moved to an offset that is 4 mod 8 inside the
# metadata FlatBuffer, which `flatbuffers::Verifier` accepts. Selecting `a` must succeed, selecting `b`
# must fail with the misalignment error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"

python3 - "$DATA_FILE" <<'EOF'
import struct
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

# pyarrow assigns dictionary ids in field order: `a` -> id 0, `b` -> id 1.
tbl = pa.table({
    'a': pa.array(['a0', 'a1', 'a2']).dictionary_encode(),
    'b': pa.array(['b0', 'b1', 'b2']).dictionary_encode(),
})
sink = pa.BufferOutputStream()
w = ipc.new_stream(sink, tbl.schema)
w.write_table(tbl)
w.close()
data = sink.getvalue().to_pybytes()

def u32(m, p):
    return struct.unpack_from('<I', m, p)[0]

def field_pos(m, table, index):
    vtable = table - struct.unpack_from('<i', m, table)[0]
    vt_size = struct.unpack_from('<H', m, vtable)[0]
    slot = vtable + 4 + 2 * index
    if slot + 2 > vtable + vt_size:
        return 0
    off = struct.unpack_from('<H', m, slot)[0]
    return table + off if off else 0

def follow(m, p):
    return p + u32(m, p)

def misalign_nodes(meta):
    """Moves `DictionaryBatch.data.nodes` to the end of the metadata, with its elements at 4 mod 8."""
    m = bytearray(meta)
    root = u32(m, 0)
    dict_batch = follow(m, field_pos(m, root, 2))       # Message.header
    batch = follow(m, field_pos(m, dict_batch, 1))      # DictionaryBatch.data
    nodes_field = field_pos(m, batch, 1)                # RecordBatch.nodes
    old = follow(m, nodes_field)
    count = u32(m, old)
    vector = bytes(m[old:old + 4 + 16 * count])         # length prefix + `FieldNode` structs (16 bytes each)
    while len(m) % 8 != 0:
        m.append(0)
    new = len(m)                                        # 0 mod 8, so the elements are at 4 mod 8
    m += vector
    while len(m) % 8 != 0:
        m.append(0)
    struct.pack_into('<I', m, nodes_field, new - nodes_field)
    return bytes(m)

def dictionary_id(meta):
    root = u32(meta, 0)
    if meta[field_pos(meta, root, 1)] != 2:             # Message.header_type == DictionaryBatch
        return None
    dict_batch = follow(meta, field_pos(meta, root, 2))
    p = field_pos(meta, dict_batch, 0)
    return struct.unpack_from('<q', meta, p)[0] if p else 0

out = bytearray()
pos = 0
patched = False
while True:
    assert u32(data, pos) == 0xFFFFFFFF
    length = u32(data, pos + 4)
    if length == 0:
        out += data[pos:pos + 8]
        break
    meta = data[pos + 8:pos + 8 + length]
    body_length = ipc.read_message(pa.BufferReader(data[pos:])).body.size
    body = data[pos + 8 + length:pos + 8 + length + body_length]
    if dictionary_id(meta) == 1:
        meta = misalign_nodes(meta)
        patched = True
    out += struct.pack('<Ii', 0xFFFFFFFF, len(meta)) + meta + body
    pos += 8 + length + body_length
assert patched, "the dictionary batch of `b` was not found"

open(path, 'wb').write(bytes(out))
EOF

echo "--- SELECT the column whose dictionary is intact succeeds ---"
${CLICKHOUSE_LOCAL} --query "SELECT a FROM file('${DATA_FILE}', 'ArrowStream') ORDER BY a"

echo "--- SELECT the column whose dictionary is misaligned fails ---"
${CLICKHOUSE_LOCAL} --query "SELECT b FROM file('${DATA_FILE}', 'ArrowStream') FORMAT Null" 2>&1 \
    | grep -oF 'misaligned record batch vector' | head -1 || echo 'FAIL: expected a misalignment error'

rm -f "$DATA_FILE"
