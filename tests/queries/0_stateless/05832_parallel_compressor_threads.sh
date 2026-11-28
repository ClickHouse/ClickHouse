#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse compressor --threads N` ("number of threads for parallel compression",
# programs/compressor/Compressor.cpp) compresses the blocks of one stream in a thread
# pool instead of one by one. The pool keeps up to N uncompressed buffers and reuses
# them in turn, but it must still emit the blocks in order: the compressed stream has
# to be byte for byte the one `--threads 1` writes, and it has to decompress back to
# the input. Both are checked with more blocks than buffers.

set -e

data="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.data"
serial="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.serial"
parallel="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parallel"
decoded="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.decoded"

# About 0.6 MB of deterministic input.
seq 1 100000 > "$data"

roundtrip()
{
    local threads="$1"
    shift

    $CLICKHOUSE_COMPRESSOR --threads 1 "$@" --input "$data" --output "$serial"
    $CLICKHOUSE_COMPRESSOR --threads "$threads" "$@" --input "$data" --output "$parallel"
    $CLICKHOUSE_COMPRESSOR --decompress --input "$parallel" --output "$decoded"

    if cmp -s "$serial" "$parallel"
    then
        echo "threads=$threads: same stream as threads=1"
    else
        echo "threads=$threads: stream differs from threads=1"
    fi

    if cmp -s "$decoded" "$data"
    then
        echo "threads=$threads: decompressed $(wc -l < "$decoded") rows back unchanged"
    else
        echo "threads=$threads: decompressed data differs from the input"
    fi
}

# 36 blocks over 4 buffers: the buffers are created one by one, then reused in turn.
roundtrip 4 --block-size 16384

# 144 blocks over 2 buffers, with a codec that is not the default one.
roundtrip 2 --block-size 4096 --codec 'ZSTD(1)'

rm -f "$data" "$serial" "$parallel" "$decoded"
