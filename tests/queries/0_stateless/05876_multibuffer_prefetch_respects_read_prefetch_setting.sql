-- Tags: long, no-fasttest, no-random-settings, no-parallel-replicas
-- - long -- multibuffer reads issue one S3 request per column stream; the total adds up under the flaky check
-- - no-fasttest -- requires S3
-- - no-random-settings -- asserts exact prefetch counts under specific settings
-- - no-parallel-replicas -- other replicas may do prefetch

-- Force multibuffer even for parts with a single granule
SET merge_tree_compact_parts_min_granules_to_multibuffer_read = 1;

DROP TABLE IF EXISTS data_remote;

CREATE TABLE data_remote
(
    c1 String,
    c2 String,
    c0 String
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY ()
SETTINGS
    disk='s3_cache',
    min_bytes_for_wide_part = 10e9,
    min_bytes_for_full_part_storage = 0,
    serialization_info_version = 'basic',
    string_serialization_version = 'single_stream';

INSERT INTO data_remote SELECT * FROM generateRandom() LIMIT 50;

-- The prefetched read pool is selected independently of remote_filesystem_read_prefetch, so the
-- reader must apply the setting itself. Rows 1 and 3 assert prefetching is off when disabled;
-- rows 2 and 4 are the presence baseline that keeps row 1 from passing on a reader that never
-- prefetches at all.
SELECT * FROM data_remote FORMAT Null SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 1, remote_filesystem_read_prefetch = 0;
SELECT * FROM data_remote FORMAT Null SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 1, remote_filesystem_read_prefetch = 1;
SELECT * FROM data_remote FORMAT Null SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 0, remote_filesystem_read_prefetch = 0;
SELECT * FROM data_remote FORMAT Null SETTINGS allow_prefetched_read_pool_for_remote_filesystem = 0, remote_filesystem_read_prefetch = 1;

SYSTEM FLUSH LOGS query_log;
SELECT
    Settings['allow_prefetched_read_pool_for_remote_filesystem'],
    Settings['remote_filesystem_read_prefetch'],
    ProfileEvents['RemoteFSPrefetches']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase() AND type != 'QueryStart' AND query_kind = 'Select'
ORDER BY event_time_microseconds;

DROP TABLE data_remote;

-- use_prefetch reads local_filesystem_read_prefetch for a local part, so the local pool needs its
-- own arms. RemoteFSPrefetches counts only remote reads, hence system.filesystem_read_prefetches_log.
DROP TABLE IF EXISTS data_local;

CREATE TABLE data_local
(
    c1 String,
    c2 String,
    c0 String
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY ()
SETTINGS
    disk='default',
    min_bytes_for_wide_part = 10e9,
    min_bytes_for_full_part_storage = 0,
    serialization_info_version = 'basic',
    string_serialization_version = 'single_stream';

-- The local prefetched pool prefetches only from the second part onwards, so one part is not enough
SYSTEM STOP MERGES data_local;
INSERT INTO data_local SELECT * FROM generateRandom() LIMIT 50;
INSERT INTO data_local SELECT * FROM generateRandom() LIMIT 50;

SELECT * FROM data_local FORMAT Null SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool', local_filesystem_read_prefetch = 0, enable_filesystem_read_prefetches_log = 1, log_comment = '05876_local_off';
SELECT * FROM data_local FORMAT Null SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool', local_filesystem_read_prefetch = 1, enable_filesystem_read_prefetches_log = 1, log_comment = '05876_local_on';

SYSTEM FLUSH LOGS filesystem_read_prefetches_log, query_log;
SELECT '05876_local_off', count() > 0 FROM system.filesystem_read_prefetches_log
WHERE query_id IN (SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05876_local_off' AND type != 'QueryStart');
SELECT '05876_local_on', count() > 0 FROM system.filesystem_read_prefetches_log
WHERE query_id IN (SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05876_local_on' AND type != 'QueryStart');

DROP TABLE data_local;
