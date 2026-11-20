#!/usr/bin/env bash
# Tags: no-ordinary-database
# no-ordinary-database: an ATTACH with an explicit UUID needs an Atomic database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table attached with the UUID of a dropped one restarts its part names, so the query condition cache
# must not apply verdicts cached for the dropped table, also for a full-definition ATTACH ON CLUSTER.
# The caches keyed by file path still serve the dropped table there, so they are off:
# https://github.com/ClickHouse/ClickHouse/issues/133219

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT --send_logs_level=error -m -q "
    SET database_replicated_allow_explicit_uuid = 3;
    SET distributed_ddl_output_mode = 'none';
    SET use_query_condition_cache = 1;
    SET use_uncompressed_cache = 0, min_bytes_to_use_mmap_io = 0, local_filesystem_read_method = 'pread';
    SET use_page_cache_for_local_disks = 0, use_page_cache_for_disks_without_file_cache = 0;

    CREATE TABLE tab UUID '$uuid' (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO tab VALUES (1, 'pear');
    SELECT sum(k) FROM tab WHERE length(s) = 5;
    DROP TABLE tab SYNC;

    ATTACH TABLE tab UUID '$uuid' ON CLUSTER test_shard_localhost (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO tab VALUES (2, 'apple');
    SELECT sum(k) FROM tab WHERE length(s) = 5;
    SELECT sum(k) FROM tab WHERE length(s) = 5 SETTINGS use_query_condition_cache = 0;

    DROP TABLE tab SYNC;"
