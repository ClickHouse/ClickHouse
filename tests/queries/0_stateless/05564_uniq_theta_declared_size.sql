-- Tags: no-fasttest
-- no-fasttest: compiled without datasketches

-- A `uniqTheta` state declaring a sketch of 1 GiB with no bytes, or 1 MB of them, after the size.
SELECT finalizeAggregation(CAST(unhex('8080808004'), 'AggregateFunction(uniqTheta, UInt64)')) SETTINGS log_comment = 'uniq_theta_declared_size_1'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('8080808004'), 'AggregateFunction(uniqTheta, UInt64, String)')) SETTINGS log_comment = 'uniq_theta_declared_size_2'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('8080808004') || repeat('a', 1000000), 'AggregateFunction(uniqTheta, UInt64)')) SETTINGS log_comment = 'uniq_theta_declared_size_3'; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('8180808004'), 'AggregateFunction(uniqTheta, UInt64)')); -- { serverError TOO_LARGE_ARRAY_SIZE }

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, memory_usage < 100000000
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND startsWith(log_comment, 'uniq_theta_declared_size_')
ORDER BY log_comment;

-- A state read back from a table arrives in 4-byte blocks, so it is assembled from many reads.
DROP TABLE IF EXISTS t_uniq_theta_declared_size;
CREATE TABLE t_uniq_theta_declared_size (s AggregateFunction(uniqTheta, UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 4, max_compress_block_size = 4;
INSERT INTO t_uniq_theta_declared_size SELECT uniqThetaState(number) FROM numbers(100000);
SELECT uniqThetaMerge(s) = (SELECT uniqThetaMerge(s) FROM (SELECT uniqThetaState(number) AS s FROM numbers(100000))) FROM t_uniq_theta_declared_size;
DROP TABLE t_uniq_theta_declared_size;
