-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-parallel
-- - no-fasttest -- S3 is required
-- - no-random-settings, no-random-merge-tree-settings -- the counts of files and requests depend on the layout and the read settings
-- - no-parallel -- SYSTEM DROP MARK CACHE

-- A Compact part is written in stripes of `compact_parts_max_granules_to_buffer` granules, and a column is read from
-- its own buffer, so reading two columns out of five reads about their data only: one read request per column per stripe.

SET allow_prefetched_read_pool_for_remote_filesystem = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0;
SET max_threads = 1;
SET remote_read_min_bytes_for_seek = 100000;
SET merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 163840;
SET merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 251658240;

DROP TABLE IF EXISTS t_compact_bytes_local;
DROP TABLE IF EXISTS t_compact_bytes_s3;
DROP TABLE IF EXISTS t_compact_bytes_s3_one_granule;

CREATE TABLE t_compact_bytes_local(c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', auto_statistics_types = '',
compact_parts_max_granules_to_buffer = 32, storage_policy = 'default', write_marks_for_substreams_in_compact_parts = 0;

INSERT INTO t_compact_bytes_local SELECT number, number, number, number, number FROM numbers(512 * 32 * 40);
OPTIMIZE TABLE t_compact_bytes_local FINAL;

SYSTEM DROP MARK CACHE;
SELECT count() FROM t_compact_bytes_local WHERE NOT ignore(c2, c4);
SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['FileOpen'],
    ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < ProfileEvents['ReadCompressedBytes'] * 1.1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ilike '%SELECT count() FROM t_compact_bytes_local WHERE NOT ignore(c2, c4)%';

CREATE TABLE t_compact_bytes_s3(c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', auto_statistics_types = '',
compact_parts_max_granules_to_buffer = 32, storage_policy = 's3_no_cache';

INSERT INTO t_compact_bytes_s3 SELECT number, number, number, number, number FROM numbers(512 * 32 * 40);

SYSTEM DROP MARK CACHE;
OPTIMIZE TABLE t_compact_bytes_s3 FINAL;

SYSTEM DROP MARK CACHE;
SELECT count() FROM t_compact_bytes_s3 WHERE NOT ignore(c2, c4);
SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['S3ReadRequestsCount'], -- 81 = 40 stripes * 2 columns + 1 file with marks
    ProfileEvents['ReadBufferFromS3Bytes'] < ProfileEvents['ReadCompressedBytes'] * 1.1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ilike '%SELECT count() FROM t_compact_bytes_s3 WHERE NOT ignore(c2, c4)%';

-- The merge reads a small part as a whole with one request, plus one for the marks.
SELECT
    ProfileEvents['S3ReadRequestsCount'],
    ProfileEvents['ReadBufferFromS3Bytes'] < ProfileEvents['ReadCompressedBytes'] * 1.1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ilike '%OPTIMIZE TABLE t_compact_bytes_s3 FINAL%';

CREATE TABLE t_compact_bytes_s3_one_granule(c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', auto_statistics_types = '',
compact_parts_max_granules_to_buffer = 1, compact_parts_max_bytes_to_buffer = 0,
storage_policy = 's3_no_cache';

INSERT INTO t_compact_bytes_s3_one_granule SELECT number, number, number, number, number FROM numbers(512 * 32 * 40);
OPTIMIZE TABLE t_compact_bytes_s3_one_granule FINAL;

SYSTEM DROP MARK CACHE;
SELECT count() FROM t_compact_bytes_s3_one_granule WHERE NOT ignore(c2, c4);
SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['S3ReadRequestsCount'],
    ProfileEvents['ReadBufferFromS3Bytes'] < ProfileEvents['ReadCompressedBytes'] * 1.1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query ilike '%OPTIMIZE TABLE t_compact_bytes_s3_one_granule FINAL%';

DROP TABLE IF EXISTS t_compact_bytes_local;
DROP TABLE IF EXISTS t_compact_bytes_s3;
DROP TABLE IF EXISTS t_compact_bytes_s3_one_granule;

CREATE TABLE test_error(c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G',
compact_parts_max_granules_to_buffer = 0, compact_parts_max_bytes_to_buffer = 0,
storage_policy = 's3_no_cache'; -- { serverError BAD_ARGUMENTS }
