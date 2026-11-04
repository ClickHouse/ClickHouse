#!/usr/bin/env bash
# The `Npy` input format reads NumPy unicode arrays (`<U<n>`, `>U<n>`: UTF-32) as UTF-8, and keeps zero bytes inside
# `|S<n>` byte strings, removing only the trailing padding. It used to delete every zero byte instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

# The files are written by hand in the `.npy` layout (what `np.save` writes), so the test doesn't need NumPy.
python3 - "$DATA_FILE" <<'PY'
import struct, sys

def save(path, descr, count, data):
    header = "{'descr': '%s', 'fortran_order': False, 'shape': (%d,), }" % (descr, count)
    header += ' ' * (63 - (10 + len(header)) % 64) + '\n'
    with open(path, 'wb') as f:
        f.write(b'\x93NUMPY\x01\x00' + struct.pack('<H', len(header)) + header.encode() + data)

def utf32(values, width, order):
    return b''.join(''.join(v).ljust(width, '\0').encode('utf-32-' + order) for v in values)

words = ['ab', 'äö', '日本', '😀x', 'Ā', 'a\0b', '']
base = sys.argv[1]
save(base + '.u_le.npy', '<U3', len(words), utf32(words, 3, 'le'))
save(base + '.u_be.npy', '>U3', len(words), utf32(words, 3, 'be'))
save(base + '.s.npy', '|S3', 4, b'a\x00b' + b'\x00xy' + b'xyz' + b'q\x00\x00')
save(base + '.invalid.npy', '<U1', 1, struct.pack('<I', 0x110000))
PY

for FILE in u_le u_be
do
    echo "--- ${FILE}"
    ${CLICKHOUSE_LOCAL} --query "SELECT array, hex(array), isValidUTF8(array) FROM file('${DATA_FILE}.${FILE}.npy', Npy)"
done
echo "--- byte strings"
${CLICKHOUSE_LOCAL} --query "SELECT hex(array) FROM file('${DATA_FILE}.s.npy', Npy)"
echo "--- invalid code point"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}.invalid.npy', Npy)" 2>&1 | grep -o -m1 'INCORRECT_DATA'
