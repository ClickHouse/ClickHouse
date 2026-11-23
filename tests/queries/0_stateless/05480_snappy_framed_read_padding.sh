#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading a framed-snappy text file without parallel parsing must not read past the decompressed chunk.
# Each case prints the type of the first chunk (00 = compressed, 01 = uncompressed) and the read result.

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

function check()
{
    local file="${DIR}/$1.snappy"
    ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO TABLE FUNCTION file('${file}', 'LineAsString', 'line String', 'snappy')
    $2
    SETTINGS snappy_mode = 'framed';
    "
    od -A n -t x1 -j 10 -N 1 "${file}" | tr -d ' '
    ${CLICKHOUSE_CLIENT} -q "
    SELECT sum(length(line)), count() FROM file('${file}', 'LineAsString', 'line String', 'snappy')
    SETTINGS snappy_mode = 'framed', input_format_parallel_parsing = 0;
    "
}

check uncompressed "SELECT toString(number) FROM numbers(50)"
check compressed "SELECT repeat('a', number % 13) || toString(number) FROM numbers(20)"
