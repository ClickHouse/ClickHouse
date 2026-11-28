#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# A field literally named `t.u` and the child `u` of a struct `t` have the same dotted name, and here they share
# one dictionary id. The values of a shared dictionary are decoded once for each field position and requested
# type: the child takes the type that the requested `t Tuple(...)` gives it, while the field named `t.u` takes the
# type requested for `t.u`. Before the fix both fields read the decoding of whichever came first in the schema,
# so the field named `t.u` read the `UUID` values of the child. Both schema orders are covered.
#
# pyarrow gives every field its own dictionary id, so each stream is rewritten message by message: the second
# dictionary-encoded field is pointed at the first one's dictionary id in the schema, and its own dictionary
# batch is dropped. pyarrow reads the rewritten stream back as the independent check that it is valid.

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
values = pa.array([bytes.fromhex("20010db8000000000000000000000001"), bytes.fromhex("20010db8000000000000000000000002")],
                  type=pa.binary(16))
child = pa.StructArray.from_arrays([pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int32()), values)], names=["u"])
literal = pa.DictionaryArray.from_arrays(pa.array([1, 0], type=pa.int32()), values)

def framed(metadata, body):
    metadata = metadata + b"\x00" * (-len(metadata) % 8)
    return b"\xff\xff\xff\xff" + struct.pack("<i", len(metadata)) + metadata + body

for name, columns, names in (("struct_first", [child, literal], ["t", "t.u"]), ("literal_first", [literal, child], ["t.u", "t"])):
    batch = pa.record_batch(columns, names=names)
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as w:
        w.write_batch(batch)

    # schema, the dictionary batches of ids 0 and 1, then the record batch
    reader = ipc.MessageReader.open_stream(sink.getvalue())
    messages = []
    while True:
        try:
            message = reader.read_next_message()
        except StopIteration:
            break
        if message is None:
            break
        messages.append(message)
    assert len(messages) == 4, len(messages)

    # Point the second dictionary-encoded field at the first one's dictionary (id 1 -> 0). The ids are the only
    # int64 values in the schema; pyarrow's read-back below verifies the result.
    schema = bytearray(messages[0].metadata.to_pybytes())
    for i in range(0, len(schema) - 7, 8):
        if struct.unpack_from("<q", schema, i)[0] == 1:
            struct.pack_into("<q", schema, i, 0)

    stream = framed(bytes(schema), b"")
    for message in (messages[1], messages[3]):
        stream += framed(message.metadata.to_pybytes(), message.body.to_pybytes() if message.body is not None else b"")
    stream += b"\xff\xff\xff\xff\x00\x00\x00\x00"
    open(f"{out}/{name}.arrows", "wb").write(stream)

    table = ipc.open_stream(pa.py_buffer(stream)).read_all()
    assert table.column("t.u").chunk(0).indices.to_pylist() == [1, 0]
    assert table.column("t").chunk(0).field("u").indices.to_pylist() == [0, 1]
PYEOF

for NAME in struct_first literal_first; do
    FILE="${TMP_DIR}/${NAME}.arrows"
    echo "--- ${NAME}: t Tuple(u UUID), t.u String"
    ${CLICKHOUSE_LOCAL} -q "SELECT t, hex(\`t.u\`), length(\`t.u\`) FROM file('${FILE}', ArrowStream, 't Tuple(u UUID), \`t.u\` String')"
    echo "--- ${NAME}: t Tuple(u UUID), t.u IPv6"
    ${CLICKHOUSE_LOCAL} -q "SELECT t, \`t.u\` FROM file('${FILE}', ArrowStream, 't Tuple(u UUID), \`t.u\` IPv6')"
    echo "--- ${NAME}: t Tuple(u String), t.u UUID"
    ${CLICKHOUSE_LOCAL} -q "SELECT hex(tupleElement(t, 'u')), \`t.u\` FROM file('${FILE}', ArrowStream, 't Tuple(u String), \`t.u\` UUID')"
done
