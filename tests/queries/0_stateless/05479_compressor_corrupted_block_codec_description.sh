#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `[checksum: 16][method: 1][size_compressed: 4][size_decompressed: 4][payload]`, method `0x91` is `Multiple`.
# The payload chains two `Gorilla` codecs (`0x95`); the inner block declares 2621440 decompressed bytes and decodes to none.
printf '%s' "000000000000000000000000000000009119000000f91400000295959593820401000028000292829a" | xxd -r -p > "${CLICKHOUSE_TMP}/05479_corrupted.compressed"
echo -n 'corrupted Gorilla block: '
$CLICKHOUSE_COMPRESSOR --decompress --no-checksum-validation --input "${CLICKHOUSE_TMP}/05479_corrupted.compressed" --output "${CLICKHOUSE_TMP}/05479_corrupted.decompressed" 2>&1 | grep -o -m1 -F "for codec 'Gorilla(1)'. (CORRUPTED_DATA)"
rm -f "${CLICKHOUSE_TMP}"/05479_corrupted.*
