-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs S3

-- A point read of one granule of a striped Compact part must read only the requested column, also when the granule
-- is the last one of its stripe or of the part: then the next granule is in another stripe, and the reader has to
-- recognize the stripe by the previous granule. Otherwise one buffer is used for all columns, and on S3 it reads the
-- data of the column `s`, which lies between the columns `a` and `b`.

DROP TABLE IF EXISTS t_compact_point_read;

CREATE TABLE t_compact_point_read (a UInt64, s String, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 128, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
    compact_parts_max_granules_to_buffer = 4, compact_parts_max_bytes_to_buffer = '128Mi', storage_policy = 's3_no_cache';

INSERT INTO t_compact_point_read SELECT number, randomString(4096), number FROM numbers(4096);

SET max_threads = 1, enable_parallel_replicas = 0;

-- The granules are: 2 (in the middle of the first stripe), 3 (the last one of the first stripe), 31 (the last one of the part).
SELECT sum(b) FROM t_compact_point_read PREWHERE a = 300 SETTINGS log_comment = '05453_middle';
SELECT sum(b) FROM t_compact_point_read PREWHERE a = 400 SETTINGS log_comment = '05453_stripe_end';
SELECT sum(b) FROM t_compact_point_read PREWHERE a = 4000 SETTINGS log_comment = '05453_part_end';

SYSTEM FLUSH LOGS query_log;

-- The data of the column `s` is about 512 KiB per granule. It must not be read.
SELECT log_comment, ProfileEvents['ReadBufferFromS3Bytes'] < 100000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment IN ('05453_middle', '05453_stripe_end', '05453_part_end')
ORDER BY log_comment;

DROP TABLE t_compact_point_read;
