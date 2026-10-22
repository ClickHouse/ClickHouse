#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `complex_key_ssd_cache` dictionary with tiny partitions, so that inserting keys wraps around the partitions
# and the storage reads the old keys back from the file to remove them from the index before overwriting them.
# The keys read back from the file must remain valid while they are used for the lookups in the index.
# The values read back from the file must not be truncated.

$CLICKHOUSE_CLIENT --query="
    DROP DICTIONARY IF EXISTS ssd_cache_complex_key_reuse_dict;
    DROP TABLE IF EXISTS ssd_cache_complex_key_reuse_source;

    CREATE TABLE ssd_cache_complex_key_reuse_source (k1 String, k2 UInt64, v String) ENGINE = Memory;
    INSERT INTO ssd_cache_complex_key_reuse_source SELECT toString(number), number, 'value_' || toString(number) FROM numbers(3000);

    CREATE DICTIONARY ssd_cache_complex_key_reuse_dict
    (
        k1 String,
        k2 UInt64,
        v String DEFAULT ''
    )
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(TABLE 'ssd_cache_complex_key_reuse_source' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1000 MAX 1000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 4096 WRITE_BUFFER_SIZE 4096 MAX_PARTITIONS_COUNT 2 PATH '$USER_FILES_PATH/${CLICKHOUSE_DATABASE}_ssd_cache_complex_key_reuse'));

    SELECT count(), countIf(v = 'value_' || toString(number)) FROM (SELECT number, dictGet('ssd_cache_complex_key_reuse_dict', 'v', (toString(number), number)) AS v FROM numbers(2000));
    SELECT count(), countIf(v = 'value_' || toString(number)) FROM (SELECT number, dictGet('ssd_cache_complex_key_reuse_dict', 'v', (toString(number), number)) AS v FROM numbers(2000));
    SELECT count(), countIf(v = 'value_' || toString(number)), countIf(v = '') FROM (SELECT number, dictGet('ssd_cache_complex_key_reuse_dict', 'v', (toString(number), number)) AS v FROM numbers(2000, 2000));
    SELECT sum(dictHas('ssd_cache_complex_key_reuse_dict', (toString(number), number))) FROM numbers(4000);

    DROP DICTIONARY ssd_cache_complex_key_reuse_dict;
    DROP TABLE ssd_cache_complex_key_reuse_source;
"
