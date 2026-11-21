#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A block header: [16 B checksum][0x82 LZ4][LE size_compressed][LE size_decompressed], with no payload.
echo -n 'block declaring a 1 GiB compressed size: '
printf '%s' "00000000000000000000000000000000820000004008000000" | xxd -r -p \
    | ${CLICKHOUSE_COMPRESSOR} --decompress 2>&1 | grep -o -m1 'Too large size_compressed_without_checksum'

echo -n 'block declaring a 1 GiB decompressed size: '
printf '%s' "00000000000000000000000000000000821300000000000040" | xxd -r -p \
    | ${CLICKHOUSE_COMPRESSOR} --decompress 2>&1 | grep -o -m1 'Too large size_decompressed'

# A block size above the limit is capped, so the data stays readable.
echo -n 'round trip with a 300 MB block size: '
head -c 300000000 /dev/zero | ${CLICKHOUSE_COMPRESSOR} --block-size 300000000 | ${CLICKHOUSE_COMPRESSOR} --decompress | wc -c

echo 'decompressed block sizes with 2 threads:'
head -c 300000000 /dev/zero | ${CLICKHOUSE_COMPRESSOR} --threads 2 --block-size 300000000 | ${CLICKHOUSE_COMPRESSOR} --stat | cut -f2

# A chain of expanding codecs could compress a 256 MiB block to more than 512 MiB, so the block is halved.
FPC_CHAIN="FPC FPC FPC FPC FPC FPC FPC"
echo -n 'round trip with a chain of seven FPC codecs: '
# shellcheck disable=SC2086
head -c 300000000 /dev/zero | ${CLICKHOUSE_COMPRESSOR} --block-size 300000000 --codec $FPC_CHAIN | ${CLICKHOUSE_COMPRESSOR} --decompress | wc -c

echo 'decompressed block sizes with a chain of seven FPC codecs:'
# shellcheck disable=SC2086
head -c 300000000 /dev/zero | ${CLICKHOUSE_COMPRESSOR} --block-size 300000000 --codec $FPC_CHAIN | ${CLICKHOUSE_COMPRESSOR} --stat | cut -f2
