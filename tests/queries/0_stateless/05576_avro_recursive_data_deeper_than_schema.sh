#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

# The schema of a file is a few levels deep, but the data of a recursive record is as deep as it has links. A field that is not
# selected is skipped, and skipping data that nests deeper than the schema does (more than 256 levels, the limit of the depth of
# a schema) must keep working: only the nesting that exhausts the stack or the memory is an error.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# Three rows of `id`, a recursive field that is not read, and `tail`, which is read after it, so a row that was skipped to its end
# and the next one are checked together. `list`: a recursive record L { next: [null, L] }. `tree`: a recursive record
# T { children: array<T> } with one child on each level.
function gen()
{
    python3 -c "
import json, sys
dst, mode, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
def zz(v):
    v = (v << 1) ^ (v >> 63)
    v &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = v & 0x7f
        v >>= 7
        out.append(b | 0x80 if v else b)
        if not v:
            break
    return bytes(out)
def ab(b):
    return zz(len(b)) + b
if mode == 'list':
    recursive = {'type': 'record', 'name': 'L', 'fields': [{'name': 'next', 'type': ['null', 'L']}]}
    value = zz(1) * n + zz(0)
else:
    recursive = {'type': 'record', 'name': 'T', 'fields': [{'name': 'children', 'type': {'type': 'array', 'items': 'T'}}]}
    value = zz(1) * n + zz(0) + zz(0) * n
fields = [{'name': 'id', 'type': 'long'}, {'name': 'deep', 'type': recursive}, {'name': 'tail', 'type': 'string'}]
rows = [zz(i) + value + ab(b'after-%d' % i) for i in (42, 43, 44)]
payload = b''.join(rows)
schema = json.dumps({'type': 'record', 'name': 'root', 'fields': fields}, separators=(',', ':')).encode()
meta = zz(2) + ab(b'avro.schema') + ab(schema) + ab(b'avro.codec') + ab(b'null') + zz(0)
sync = bytes(16)
open(dst, 'wb').write(b'Obj\x01' + meta + sync + zz(len(rows)) + zz(len(payload)) + payload + sync)
" "$@"
}

for shape in list tree; do
    for links in 10 300 1000; do
        gen "$DIR/${shape}_${links}.avro" $shape $links
        echo "$shape $links"
        $CLICKHOUSE_LOCAL -q "SELECT id, tail FROM file('$DIR/${shape}_${links}.avro', Avro, 'id Int64, tail String')"
        $CLICKHOUSE_LOCAL -q "SELECT count(), sum(id) FROM file('$DIR/${shape}_${links}.avro', Avro, 'id Int64')"
    done
done

rm -rf "$DIR"
