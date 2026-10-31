#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ssd_cache` dictionaries with two partitions of one block (three values) each. The source returns key 5 twice
# in one request, so its latest value is cached in the second partition while the first one keeps an old copy.
# After the cache wraps around, writing the first partition to the file must not point key 5 at another key's value.

function get()
{
    for k in "$@"; do
        echo "SELECT dictGet('ssd_dup_simple', 'v', toUInt64($k)), dictGet('ssd_dup_complex', 'v', (toString($k), toUInt64($k))) FORMAT Null;"
    done
}

check="SELECT splitByChar(':', dictGet('ssd_dup_simple', 'v', toUInt64(5)))[1], splitByChar(':', dictGet('ssd_dup_complex', 'v', ('5', toUInt64(5))))[1];"

trap 'rm -f "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE:?}_ssd_dup_simple.bin" "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE:?}_ssd_dup_complex.bin"' EXIT

$CLICKHOUSE_CLIENT --query="
    DROP DICTIONARY IF EXISTS ssd_dup_simple;
    DROP DICTIONARY IF EXISTS ssd_dup_complex;
    DROP TABLE IF EXISTS ssd_dup_source;

    -- TinyLog returns the rows in insertion order: key 5 comes again after key 9.
    CREATE TABLE ssd_dup_source (k1 String, k2 UInt64, v String) ENGINE = TinyLog;
    INSERT INTO ssd_dup_source SELECT toString(k), k, toString(k) || ':' || repeat('x', 1200) FROM (SELECT toUInt64(arrayJoin([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 5])) AS k);

    CREATE DICTIONARY ssd_dup_simple (k2 UInt64, v String DEFAULT '')
    PRIMARY KEY k2
    SOURCE(CLICKHOUSE(TABLE 'ssd_dup_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 2 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_dup_simple'));

    CREATE DICTIONARY ssd_dup_complex (k1 String, k2 UInt64, v String DEFAULT '')
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(TABLE 'ssd_dup_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 1000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 2 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_dup_complex'));

    $(get 0 1 2 3 4)
    -- One request for keys 5..9; the source returns 5, 6, 7, 8, 9, 5.
    SELECT dictGet('ssd_dup_simple', 'v', k), dictGet('ssd_dup_complex', 'v', (toString(k), k)) FROM (SELECT toUInt64(arrayJoin([5, 6, 7, 8, 9])) AS k) FORMAT Null;

    -- From here on a value served from the cache stays 5, while a new fetch would return new_5.
    TRUNCATE TABLE ssd_dup_source;
    INSERT INTO ssd_dup_source SELECT toString(number), number, 'new_' || toString(number) || ':' || repeat('x', 1200) FROM numbers(20);

    $(get 10 11)
    $check
    $(get 12 13 14 15 16 17)
    $check

    DROP DICTIONARY ssd_dup_simple;
    DROP DICTIONARY ssd_dup_complex;
    DROP TABLE ssd_dup_source;
"
