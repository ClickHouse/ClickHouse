#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format needs USE_AVRO.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

AVRO_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.avro"
trap 'rm -f "${AVRO_FILE}"' EXIT

# A: the Iceberg data file shape, `["null", fixed(16)]` with `logicalType: uuid`, written by hand.
python3 - "${AVRO_FILE}" << 'PY'
import json, sys, uuid

def avro_long(n):
    n = (n << 1) ^ (n >> 63)
    out = b''
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out += bytes([b | 0x80])
        else:
            return out + bytes([b])

def avro_bytes(b):
    return avro_long(len(b)) + b

schema = {'type': 'record', 'name': 'table', 'fields': [
    {'name': 'id', 'type': 'int'},
    {'name': 'u', 'type': ['null', {'type': 'fixed', 'name': 'uuid_fixed', 'size': 16, 'logicalType': 'uuid'}], 'default': None}]}
rows = [(1, '00112233-4455-6677-8899-aabbccddeeff'), (2, '12345678-1234-5678-1234-567812345678'),
        (3, '00000000-0000-0000-0000-000000000000'), (4, None), (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff')]
body = b''.join(avro_long(i) + (avro_long(0) if u is None else avro_long(1) + uuid.UUID(u).bytes) for i, u in rows)
sync = bytes(range(16))
header = b'Obj\x01' + avro_long(2) + avro_bytes(b'avro.schema') + avro_bytes(json.dumps(schema).encode()) \
    + avro_bytes(b'avro.codec') + avro_bytes(b'null') + avro_long(0) + sync
with open(sys.argv[1], 'wb') as f:
    f.write(header + avro_long(len(rows)) + avro_long(len(body)) + body + sync)
PY
${CLICKHOUSE_LOCAL} -q "SELECT id, u FROM file('${AVRO_FILE}', 'Avro', 'id Int32, u Nullable(UUID)') ORDER BY id"

# B: a plain `fixed(16)` without a logical type, also inside an array.
${CLICKHOUSE_LOCAL} -q "SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff')) AS u, [UUIDToNum(toUUID('12345678-9abc-def0-1234-56789abcdef0'))] AS arr FORMAT Avro" \
    | ${CLICKHOUSE_LOCAL} --input-format Avro -S 'u UUID, arr Array(UUID)' -q 'SELECT * FROM table'

# C: the 36-character text form in a `fixed(36)` is still accepted.
${CLICKHOUSE_LOCAL} -q "SELECT toFixedString('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 36) AS u FORMAT Avro" \
    | ${CLICKHOUSE_LOCAL} --input-format Avro -S 'u UUID' -q 'SELECT * FROM table'

# D: any other size is rejected.
${CLICKHOUSE_LOCAL} -q "SELECT toFixedString('01234567', 8) AS u FORMAT Avro" \
    | ${CLICKHOUSE_LOCAL} --input-format Avro -S 'u UUID' -q 'SELECT * FROM table' 2>&1 | grep -o -m1 'CANNOT_PARSE_UUID'
