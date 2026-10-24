#!/usr/bin/env bash
# Tags: no-fasttest
# A cursor with an absurdly deep structure must be rejected with an exception instead of exhausting the stack:
# both in `STREAM CURSOR` of a query and in the refresh cursor that is stored in Iceberg metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DEEP_PATH="a$(printf '.a%.0s' {1..100000})"
echo "SELECT 1 FROM system.one STREAM CURSOR {'${DEEP_PATH}': 1}" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_query_size=10000000" --data-binary @- | grep -o -m1 'BAD_ARGUMENTS'

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_deep_refresh_cursor/"
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE src (id Int64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    CREATE TABLE tgt (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}');
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT id FROM src;
    INSERT INTO src VALUES (1), (2);
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT count() FROM tgt;
"

# Replace the refresh cursor of the current snapshot with a forged one in a new metadata version.
forge_cursor()
{
    python3 -I - "${TABLE_PATH}metadata" "$1" <<'EOF'
import base64, json, os, re, struct, sys

metadata_dir, kind = sys.argv[1], sys.argv[2]
versions = {int(re.match(r'v?(\d+)', name).group(1)): name for name in os.listdir(metadata_dir) if name.endswith('.metadata.json')}
last = max(versions)
with open(os.path.join(metadata_dir, versions[last])) as f:
    metadata = json.load(f)

if kind == 'nested':
    # Binary `Field`: 100000 nested single-element arrays around `UInt64` 1.
    payload = (bytes([17]) + struct.pack('<Q', 1)) * 100000 + bytes([1, 1])
else:
    # Binary `Field`: Map with one element ('a.a.a...', 1).
    path = ('a.' * 100000 + 'a').encode()
    varuint = bytearray()
    n = len(path)
    while n >= 0x80:
        varuint.append((n & 0x7F) | 0x80)
        n >>= 7
    varuint.append(n)
    payload = bytes([26]) + struct.pack('<Q', 1) + bytes([18]) + struct.pack('<Q', 2) + bytes([16]) + bytes(varuint) + path + bytes([2, 2])

snapshot = next(s for s in metadata['snapshots'] if s['snapshot-id'] == metadata['current-snapshot-id'])
assert 'clickhouse.refresh-cursor' in snapshot['summary']
snapshot['summary']['clickhouse.refresh-cursor'] = base64.b64encode(payload).decode()
with open(os.path.join(metadata_dir, 'v%d.metadata.json' % (last + 1)), 'w') as f:
    json.dump(metadata, f)
EOF
}

for kind in nested dotted
do
    forge_cursor "$kind"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW mv; SYSTEM WAIT VIEW mv;" 2>&1 | grep -o -m1 -E 'TOO_DEEP_RECURSION|BAD_ARGUMENTS'
done

${CLICKHOUSE_CLIENT} --query "SELECT 'still alive'; DROP TABLE mv; DROP TABLE tgt; DROP TABLE src;"
rm -rf "${TABLE_PATH}"
