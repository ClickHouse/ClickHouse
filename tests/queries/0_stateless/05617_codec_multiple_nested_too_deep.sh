#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A block of the `Multiple` codec may hold a `Multiple` block: versions before 26.10 wrote that for
# `CODEC(Delta, Default)` when the default codec was a chain. A deeper nesting is rejected, because
# decompression recurses once per level, and a server used to overflow its stack on a small block
# with tens of thousands of levels.

BLOCK="$CUR_DIR"/05617_codec_multiple_nested_too_deep.python
URL="${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1"

echo -n 'not nested: '
python3 "$BLOCK" http_body 1 | ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @-

echo -n 'nested once: '
python3 "$BLOCK" http_body 2 | ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @-

echo -n 'nested twice: '
python3 "$BLOCK" http_body 3 | ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @- 2>&1 |
    grep -oF 'Codec Multiple is nested inside a nested codec Multiple'

echo -n 'nested 50000 times: '
python3 "$BLOCK" http_body 50001 | ${CLICKHOUSE_CURL} -sS "$URL" --data-binary @- 2>&1 |
    grep -oF 'Codec Multiple is nested inside a nested codec Multiple'

# The same block as the data of an external table, the way any client can send it.
echo -n 'nested 50000 times, native protocol: '
python3 "$BLOCK" native 50001

$CLICKHOUSE_CLIENT -q "SELECT 'server alive'"
