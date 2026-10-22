#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ssd_cache` dictionaries with two partitions of one block (three values) each: after the cache wraps around,
# flushing a partition must not re-point a key whose live value was fetched again into the other partition.

function get()
{
    for k in "$@"; do
        echo "SELECT dictGet('ssd_wrap_simple', 'v', toUInt64($k)), dictGet('ssd_wrap_complex', 'v', (toString($k), toUInt64($k))) FORMAT Null;"
    done
}

check="SELECT splitByChar(':', dictGet('ssd_wrap_simple', 'v', toUInt64(5)))[1], splitByChar(':', dictGet('ssd_wrap_complex', 'v', ('5', toUInt64(5))))[1];"

$CLICKHOUSE_CLIENT --query="
    DROP DICTIONARY IF EXISTS ssd_wrap_simple;
    DROP DICTIONARY IF EXISTS ssd_wrap_complex;
    DROP TABLE IF EXISTS ssd_wrap_source;

    CREATE TABLE ssd_wrap_source (k1 String, k2 UInt64, v String) ENGINE = Memory;
    INSERT INTO ssd_wrap_source SELECT toString(number), number, toString(number) || ':' || repeat('x', 1200) FROM numbers(20);

    CREATE DICTIONARY ssd_wrap_simple (k2 UInt64, v String DEFAULT '')
    PRIMARY KEY k2
    SOURCE(CLICKHOUSE(TABLE 'ssd_wrap_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 3 MAX 3)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 2 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_wrap_simple'));

    CREATE DICTIONARY ssd_wrap_complex (k1 String, k2 UInt64, v String DEFAULT '')
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(TABLE 'ssd_wrap_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 3 MAX 3)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 2 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_wrap_complex'));

    $(get 0 1 2 3 4 5)
"

# Let the cached values expire, so that key 5 is fetched again into the other partition.
sleep 4

# After the re-fetch, the source changes: a value served from the cache stays `5`, while a new fetch would return `new_5`.
$CLICKHOUSE_CLIENT --query="
    $(get 6 7 8 9 5)
    TRUNCATE TABLE ssd_wrap_source;
    INSERT INTO ssd_wrap_source SELECT toString(number), number, 'new_' || toString(number) || ':' || repeat('x', 1200) FROM numbers(20);
    $(get 10 11)
    $check
    $(get 12 13 14 15 16 17)
    $check
    DROP DICTIONARY ssd_wrap_simple;
    DROP DICTIONARY ssd_wrap_complex;
    DROP TABLE ssd_wrap_source;
"
