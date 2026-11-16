#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Records larger than the default read buffer must still be fetched from disk: the read window
# is bounded by the size stored with the record, not by read_buffer_size / block_size.
#
# The source table is emptied once the cache is populated, so the cache is the only thing that
# can answer. Otherwise a storage that refuses to serve a record still returns the right value,
# because CacheDictionary::getColumns falls back to a synchronous source update on any miss.

$CLICKHOUSE_CLIENT --query="
    CREATE TABLE source_table
    (
        id UInt64,
        id_key String,
        value String
    )
    ENGINE = TinyLog;

    INSERT INTO source_table SELECT number, concat('id_key_', toString(number)), repeat('v', 100) FROM numbers(200);

    CREATE DICTIONARY ssd_cache_simple_key
    (
        id UInt64,
        value String DEFAULT 'default'
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source_table' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 262144 WRITE_BUFFER_SIZE 4096 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_simple'));

    CREATE DICTIONARY ssd_cache_complex_key
    (
        id UInt64,
        id_key String,
        value String DEFAULT 'default'
    )
    PRIMARY KEY id, id_key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'source_table' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 262144 WRITE_BUFFER_SIZE 4096 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_complex'));

    -- Populate the cache; the memory buffer holds one block, so most keys end up on disk.
    SELECT ignore(dictGet('ssd_cache_simple_key', 'value', number)) FROM numbers(200) FORMAT Null;
    SELECT ignore(dictGet('ssd_cache_complex_key', 'value', (number, concat('id_key_', toString(number))))) FROM numbers(200) FORMAT Null;

    TRUNCATE TABLE source_table;

    SELECT 'simple key mismatches';
    SELECT countIf(dictGet('ssd_cache_simple_key', 'value', number) != repeat('v', 100)) FROM numbers(200)
    SETTINGS log_comment = '05633_simple_${CLICKHOUSE_DATABASE}';

    SELECT 'complex key mismatches';
    SELECT countIf(dictGet('ssd_cache_complex_key', 'value', (number, concat('id_key_', toString(number)))) != repeat('v', 100)) FROM numbers(200)
    SETTINGS log_comment = '05633_complex_${CLICKHOUSE_DATABASE}';
"

# AIORead is incremented only by SSDCacheDictionaryStorage, so a non-zero count on both queries
# above proves the values came back through the on-disk read path, not from the memory buffer.
$CLICKHOUSE_CLIENT --query="
    SYSTEM FLUSH LOGS query_log;

    SELECT 'queries reading from disk';
    SELECT countIf(ProfileEvents['AIORead'] > 0)
    FROM system.query_log
    WHERE current_database = '$CLICKHOUSE_DATABASE'
      AND log_comment IN ('05633_simple_${CLICKHOUSE_DATABASE}', '05633_complex_${CLICKHOUSE_DATABASE}')
      AND type = 'QueryFinish';

    DROP DICTIONARY ssd_cache_simple_key;
    DROP DICTIONARY ssd_cache_complex_key;
    DROP TABLE source_table;
"
