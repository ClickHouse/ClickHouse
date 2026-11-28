#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: zip archives need minizip, which is not in the fast test build.

# An encrypted entry of a zip archive whose compressed size is less than the size of the encryption header
# must be rejected when restoring a backup from the archive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(cd "${CLICKHOUSE_TMP}" && pwd)/${CLICKHOUSE_TEST_UNIQUE_NAME}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

cat > "${WORK_DIR}/config.xml" <<CONFIG
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/backups</allowed_path>
    </backups>
</clickhouse>
CONFIG

# A zip archive with one stored entry `.backup` encrypted with the traditional PKWARE encryption and the
# password "password". The compressed size of the entry is 1, less than the 12-byte encryption header.
python3 - "${WORK_DIR}/backups/archive.zip" <<'PYEOF'
import struct
import sys

table = []
for n in range(256):
    c = n
    for _ in range(8):
        c = (c >> 1) ^ 0xEDB88320 if c & 1 else c >> 1
    table.append(c)

def crc32_update(crc, b):
    return (crc >> 8) ^ table[(crc ^ b) & 0xFF]

keys = [0x12345678, 0x23456789, 0x34567890]

def update_keys(b):
    keys[0] = crc32_update(keys[0], b)
    keys[1] = ((keys[1] + (keys[0] & 0xFF)) * 134775813 + 1) & 0xFFFFFFFF
    keys[2] = crc32_update(keys[2], keys[1] >> 24)

def encrypt(data):
    out = bytearray()
    for b in data:
        t = (keys[2] | 2) & 0xFFFF
        out.append(b ^ (((t * (t ^ 1)) >> 8) & 0xFF))
        update_keys(b)
    return bytes(out)

for b in b'password':
    update_keys(b)
# The last byte of the header is the high byte of the CRC (0) to pass the password check.
header = encrypt(bytes(11) + b'\x00')

name = b'.backup'
local = struct.pack('<IHHHHHIIIHH', 0x04034B50, 20, 1, 0, 0, 0, 0, 1, 1, len(name), 0) + name
data = header + b'A' * 64
central = struct.pack('<IHHHHHHIIIHHHHHII', 0x02014B50, 20, 20, 1, 0, 0, 0, 0, 1, 1, len(name), 0, 0, 0, 0, 0, 0) + name
end = struct.pack('<IHHHHIIH', 0x06054B50, 0, 0, 1, 1, len(central), len(local) + len(data), 0)
open(sys.argv[1], 'wb').write(local + data + central + end)
PYEOF

${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/config.xml" --path "${WORK_DIR}/data" -q "
    RESTORE TABLE t FROM File('${WORK_DIR}/backups/archive.zip') SETTINGS password = 'password'
" 2>&1 | grep -o -m1 'CANNOT_UNPACK_ARCHIVE'

rm -rf "${WORK_DIR}"
