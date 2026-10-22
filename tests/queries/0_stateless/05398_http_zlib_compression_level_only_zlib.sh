#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `http_zlib_compression_level` applies only to the zlib-based methods (`gzip`, `deflate`).
# Other methods use `output_format_compression_level`.

QUERY="SELECT+number+FROM+numbers(100000)"
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_response"

function fetch()
{
    local encoding=$1 params=$2 extension=$3
    local code
    code=$(${CLICKHOUSE_CURL} -sS -o "${FILE}.${extension}" -w '%{http_code}' -H "Accept-Encoding: ${encoding}" \
        "${CLICKHOUSE_URL}&enable_http_compression=1&${params}&query=${QUERY}")
    echo -n "${encoding} ${params}: ${code} "
    ${CLICKHOUSE_LOCAL} -q "SELECT count(), sum(toUInt64(line)) FROM file('${FILE}.${extension}', LineAsString)"
}

function size()
{
    ${CLICKHOUSE_CURL} -sS -H "Accept-Encoding: $1" "${CLICKHOUSE_URL}&enable_http_compression=1&$2&query=${QUERY}" | wc -c
}

fetch gzip "http_zlib_compression_level=9" gz
# Level 12 is valid for zlib with `libdeflate`, but not for `xz` (presets 0..9); it must not be passed to `xz`.
fetch xz "http_zlib_compression_level=12" xz
fetch zstd "http_zlib_compression_level=12" zst
fetch lz4 "http_zlib_compression_level=12" lz4

# `output_format_compression_level` controls the level of `zstd`, and `http_zlib_compression_level` does not.
[[ $(size zstd "http_zlib_compression_level=1") -eq $(size zstd "http_zlib_compression_level=9") ]] && echo "zstd ignores http_zlib_compression_level"
[[ $(size zstd "output_format_compression_level=1") -gt $(size zstd "output_format_compression_level=19") ]] && echo "zstd uses output_format_compression_level"

rm -f "${FILE}".*
