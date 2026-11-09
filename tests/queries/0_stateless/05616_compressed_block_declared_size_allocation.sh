#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_declared_size; CREATE TABLE t_declared_size (s String) ENGINE = Memory"

insert_url="${CLICKHOUSE_URL}&decompress=1&query=INSERT%20INTO%20t_declared_size%20FORMAT%20TSV"

# The first frame holds more than max_query_size bytes, so the query text is parsed from it alone and the
# second frame is first read by the INSERT pipeline, where max_memory_usage applies.
# Second frame: [16 B checksum, never checked][0x82 LZ4][LE size_compressed = 2^30, the largest accepted]
# [LE size_decompressed = 8] and 5 of the declared bytes.
echo '-- a truncated block declaring 1 GiB fails without allocating it'
{
    seq 1 1000 | ${CLICKHOUSE_COMPRESSOR}
    printf '\x00%.0s' {1..16}
    printf '\x82\x00\x00\x00\x40\x08\x00\x00\x00abcde'
} | ${CLICKHOUSE_CURL} -sS "${insert_url}&max_query_size=1000&max_memory_usage=300000000" --data-binary @- 2>&1 \
    | grep -o -m1 -e CANNOT_READ_ALL_DATA -e MEMORY_LIMIT_EXCEEDED

# Both blocks are larger than the 1 MiB buffer the HTTP body is read with, so neither is ever fully available.
echo '-- valid blocks larger than the read buffer'
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE t_declared_size"
${CLICKHOUSE_CLIENT} --query "SELECT hex(cityHash64(number)) FROM numbers(400000) FORMAT TSV" \
    | ${CLICKHOUSE_COMPRESSOR} --block-size 4194304 \
    | ${CLICKHOUSE_CURL} -sS "${insert_url}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(cityHash64(s)) = (SELECT sum(cityHash64(hex(cityHash64(number)))) FROM numbers(400000))
    FROM t_declared_size"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_declared_size"
