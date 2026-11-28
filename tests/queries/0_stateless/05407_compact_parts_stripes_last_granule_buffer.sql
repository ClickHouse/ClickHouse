-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs S3

-- A column of a striped Compact part is read with its own buffer, whose size is estimated from the marks. The estimate
-- must cover all substreams of the column: here the first substream (`t.s`) is big and the last one (`t.x`) is tiny.
-- For the last granule of a stripe, an estimate from the last substream alone gives a tiny buffer, and the big
-- substream is read through it with a huge number of reads.

DROP TABLE IF EXISTS t_compact_last_granule;

CREATE TABLE t_compact_last_granule (a UInt64, t Tuple(s String, x UInt8), b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '10G', storage_policy = 's3_no_cache',
    compact_parts_max_granules_to_buffer = 4, compact_parts_max_bytes_to_buffer = '128Mi',
    write_marks_for_substreams_in_compact_parts = 1, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_compact_last_granule SELECT number, (randomString(500000), 1), number FROM numbers(8);

-- The last granule of the stripe.
SELECT sum(length(t.s)), sum(t.x) FROM t_compact_last_granule WHERE a >= 6
SETTINGS max_threads = 1, enable_parallel_replicas = 0, merge_tree_compact_parts_min_granules_to_multibuffer_read = 1,
    remote_filesystem_read_method = 'threadpool', log_comment = '05407_last_granule';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RemoteFSPrefetchedReads'] + ProfileEvents['RemoteFSUnprefetchedReads'] < 100
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05407_last_granule';

DROP TABLE t_compact_last_granule;
