#!/usr/bin/env bash
# Tags: no-fasttest
# The block number in the cursor of a streaming read of a `Memory` table is an input of the query: the boundary values
# are rejected or accepted without an overflow.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_memory_cursor_boundary/"
rm -rf "${TABLE_PATH}"

# A value of the refresh cursor that the current Iceberg snapshot of the target stores. The cursor is needed to get
# the epoch of the server process and the generation of the data: a cursor of another generation is not validated.
cursor_value()
{
    python3 -I - "${TABLE_PATH}metadata" "$1" <<'PYEOF'
import base64, json, os, re, sys

metadata_dir, key = sys.argv[1], sys.argv[2].encode()
versions = {int(re.match(r'v?(\d+)', name).group(1)): name for name in os.listdir(metadata_dir) if name.endswith('.metadata.json')}
with open(os.path.join(metadata_dir, versions[max(versions)])) as f:
    metadata = json.load(f)
snapshot = next(s for s in metadata['snapshots'] if s['snapshot-id'] == metadata['current-snapshot-id'])
cursor = base64.b64decode(snapshot['summary']['clickhouse.refresh-cursor'])
# The key is followed by the type of the value (`Int64`) and the value as a zigzag-encoded varint.
pos = cursor.index(key) + len(key) + 1
value, shift = 0, 0
while True:
    byte = cursor[pos]
    value |= (byte & 0x7f) << shift
    shift += 7
    pos += 1
    if not byte & 0x80:
        break
print((value >> 1) ^ -(value & 1))
PYEOF
}

$STREAMING_CLIENT -q "
    CREATE TABLE src (v Int64) ENGINE = Memory;
    INSERT INTO src VALUES (1);
    INSERT INTO src VALUES (2);
    CREATE TABLE tgt (v Int64) ENGINE = IcebergLocal('${TABLE_PATH}');
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY AS SELECT v FROM src;
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
"

epoch=$(cursor_value epoch)
generation=$(cursor_value generation)
echo "block_number in the cursor: $(cursor_value block_number)"

read_from()
{
    $STREAMING_CLIENT -q "SELECT v FROM src STREAM BOUNDED CURSOR {'epoch': ${epoch}, 'generation': ${generation}, 'block_number': $1}" 2>&1 \
        | grep -o -m1 "BAD_ARGUMENTS" || true
}

echo "--- after the first block"
$STREAMING_CLIENT -q "SELECT v FROM src STREAM BOUNDED CURSOR {'epoch': ${epoch}, 'generation': ${generation}, 'block_number': 0}"
echo "--- after the last block"
$STREAMING_CLIENT -q "SELECT v FROM src STREAM BOUNDED CURSOR {'epoch': ${epoch}, 'generation': ${generation}, 'block_number': 1}"
echo "--- beyond the blocks"
read_from 2
# The cursor in a query has only non-negative numbers.
echo "--- the maximum"
read_from 9223372036854775807

$STREAMING_CLIENT -q "DROP TABLE mv; DROP TABLE tgt; DROP TABLE src;"
rm -rf "${TABLE_PATH}"
