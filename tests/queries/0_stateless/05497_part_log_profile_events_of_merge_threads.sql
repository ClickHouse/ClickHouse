-- Tags: no-fasttest, no-shared-merge-tree
-- no-fasttest: needs S3
-- no-shared-merge-tree: the merges of SharedMergeTree are done by other tasks

-- `system.part_log` must have the profile events of every thread that works on a merge, not only of the
-- thread that runs the merge task: a part on object storage is read by the threads of the remote read pool.
-- A merge reads all of its source parts, about as many bytes as the merged part has, so every merge must
-- show them in `ReadBufferFromS3Bytes`. A background merge may run before `OPTIMIZE`, so every merge is checked.

DROP TABLE IF EXISTS t_merge_counters;

CREATE TABLE t_merge_counters (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 's3_no_cache', auto_statistics_types = '', min_bytes_for_wide_part = 0;

INSERT INTO t_merge_counters SELECT number, randomPrintableASCII(100) FROM numbers(100000);
INSERT INTO t_merge_counters SELECT number + 100000, randomPrintableASCII(100) FROM numbers(100000);
OPTIMIZE TABLE t_merge_counters FINAL;

SYSTEM FLUSH LOGS part_log;

SELECT count() > 0, min(ProfileEvents['ReadBufferFromS3Bytes'] * 2 >= size_in_bytes)
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_merge_counters' AND event_type = 'MergeParts';

DROP TABLE t_merge_counters;
