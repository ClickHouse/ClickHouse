#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A block of the `Multiple` codec is `[0x91][compressed size: 4][decompressed size: 4][number of codecs: 1][codecs]`
# and then the block of the last codec, which may be a `Multiple` block again. Versions before 26.10 wrote that for
# `CODEC(Delta, Default)` when the default codec was a chain, so one nested level is read. A deeper nesting is rejected,
# because decompression recurses once per level.

URL="${CLICKHOUSE_URL}&decompress=1&http_native_compression_disable_checksumming_on_decompress=1"

# The bodies start with 16 bytes of a checksum that is not checked, and end with the query in a block of the `NONE` codec.
echo -n 'nested once: '
printf '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x91\x2a\0\0\0\x0b\0\0\0\x01\x91\x91\x1f\0\0\0\x0b\0\0\0\x01\x02\x02\x14\0\0\0\x0b\0\0\0SELECT '"'ok'" \
    | ${CLICKHOUSE_CURL} "$URL" --data-binary @-

echo -n 'nested twice: '
printf '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x91\x35\0\0\0\x0b\0\0\0\x01\x91\x91\x2a\0\0\0\x0b\0\0\0\x01\x91\x91\x1f\0\0\0\x0b\0\0\0\x01\x02\x02\x14\0\0\0\x0b\0\0\0SELECT '"'ok'" \
    | ${CLICKHOUSE_CURL} "$URL" --data-binary @- \
    | grep -oF 'Codec Multiple is nested inside a nested codec Multiple'

# 50000 headers of 11 bytes each, with the size of the whole block (550000) in every one: only the outermost size is checked.
echo -n 'nested 50000 times: '
(head -c 16 /dev/zero; printf '\x91\x70\x64\x08\0\x01\0\0\0\x01\x91%.0s' {1..50000}) \
    | ${CLICKHOUSE_CURL} "$URL" --data-binary @- \
    | grep -oF 'Codec Multiple is nested inside a nested codec Multiple'
