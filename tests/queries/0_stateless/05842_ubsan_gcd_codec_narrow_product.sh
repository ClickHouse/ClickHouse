#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `[checksum: 16][method: 1][size_compressed: 4][size_decompressed: 4][payload]`, method `0x9a` is `GCD`.
# The payload has width 2, no skipped bytes, GCD 65535 and quotient 65535: the product of two `UInt16` is computed
# modulo 2^16 and not in the signed `int` the operands are promoted to.
printf '%s' "000000000000000000000000000000009a0f000000020000000200ffffffff" | xxd -r -p > "${CLICKHOUSE_TMP}/05842.compressed"
$CLICKHOUSE_COMPRESSOR --decompress --no-checksum-validation --input "${CLICKHOUSE_TMP}/05842.compressed" --output "${CLICKHOUSE_TMP}/05842.decompressed"
xxd -p "${CLICKHOUSE_TMP}/05842.decompressed"
rm -f "${CLICKHOUSE_TMP}"/05842.*
