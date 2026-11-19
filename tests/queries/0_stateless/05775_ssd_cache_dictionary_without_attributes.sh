#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `ssd_cache` dictionaries without attributes must keep answering `dictHas` after the write buffer is flushed.

$CLICKHOUSE_CLIENT --query="
    DROP DICTIONARY IF EXISTS ssd_cache_key_only_dict;
    DROP DICTIONARY IF EXISTS ssd_cache_complex_key_only_dict;
    DROP TABLE IF EXISTS ssd_cache_key_only_source;

    CREATE TABLE ssd_cache_key_only_source (id UInt64, id_key String) ENGINE = TinyLog;
    INSERT INTO ssd_cache_key_only_source SELECT number, toString(number) FROM numbers(1000);

    CREATE DICTIONARY ssd_cache_key_only_dict (id UInt64)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'ssd_cache_key_only_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 1048576 WRITE_BUFFER_SIZE 4096 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_cache_key_only'));

    SELECT count() FROM numbers(1000) WHERE dictHas('ssd_cache_key_only_dict', number);
    SELECT count() FROM numbers(1000) WHERE dictHas('ssd_cache_key_only_dict', number);
    SELECT count() FROM numbers(1000, 10) WHERE dictHas('ssd_cache_key_only_dict', number);

    CREATE DICTIONARY ssd_cache_complex_key_only_dict (id UInt64, id_key String)
    PRIMARY KEY id, id_key
    SOURCE(CLICKHOUSE(TABLE 'ssd_cache_key_only_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 1048576 WRITE_BUFFER_SIZE 4096 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_cache_complex_key_only'));

    SELECT count() FROM numbers(1000) WHERE dictHas('ssd_cache_complex_key_only_dict', (number, toString(number)));
    SELECT count() FROM numbers(1000) WHERE dictHas('ssd_cache_complex_key_only_dict', (number, toString(number)));
    SELECT count() FROM numbers(1000, 10) WHERE dictHas('ssd_cache_complex_key_only_dict', (number, toString(number)));

    DROP DICTIONARY ssd_cache_key_only_dict;
    DROP DICTIONARY ssd_cache_complex_key_only_dict;
    DROP TABLE ssd_cache_key_only_source;
"
