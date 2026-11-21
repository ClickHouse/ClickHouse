-- `join_in_memory_compression_min_bytes` applies to the whole join. A build stream that stored all its blocks
-- before another one crossed the threshold has no later insert to catch it up, so its blocks must still be
-- compressed at the end of the build: every stored block is compressed, as with a zero threshold.

DROP TABLE IF EXISTS jimc_ub_left;
DROP TABLE IF EXISTS jimc_ub_small;
DROP TABLE IF EXISTS jimc_ub_big;

CREATE TABLE jimc_ub_left (k UInt64) ENGINE = Memory;
CREATE TABLE jimc_ub_small (k UInt64, pad String) ENGINE = Memory;
CREATE TABLE jimc_ub_big (k UInt64, pad String) ENGINE = Memory;

INSERT INTO jimc_ub_left SELECT number FROM numbers(10000);
-- About 1 MB in one block for the stream that finishes early, and 9 MB in many blocks for the other one.
INSERT INTO jimc_ub_small SELECT number, repeat('x', 1000) FROM numbers(1000) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000;
INSERT INTO jimc_ub_big SELECT number + 1000, repeat('y', 1000) FROM numbers(9000) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000;

SET join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 2, query_plan_join_swap_table = 'false',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, enable_join_in_memory_compression = 1;

SELECT sum(cityHash64(l.k, r.pad)) FROM jimc_ub_left AS l
INNER JOIN (SELECT k, pad FROM jimc_ub_small UNION ALL SELECT k, pad FROM jimc_ub_big) AS r ON l.k = r.k
SETTINGS join_in_memory_compression_min_bytes = 0, log_comment = '05569_zero_threshold'
FORMAT Null;

SELECT sum(cityHash64(l.k, r.pad)) FROM jimc_ub_left AS l
INNER JOIN (SELECT k, pad FROM jimc_ub_small UNION ALL SELECT k, pad FROM jimc_ub_big) AS r ON l.k = r.k
SETTINGS join_in_memory_compression_min_bytes = '4Mi', log_comment = '05569_nonzero_threshold'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT uniqExact(ProfileEvents['JoinInMemoryCompressedColumns']) = 1, min(ProfileEvents['JoinInMemoryCompressedColumns']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05569_zero_threshold', '05569_nonzero_threshold') AND type = 'QueryFinish';

DROP TABLE jimc_ub_left;
DROP TABLE jimc_ub_small;
DROP TABLE jimc_ub_big;
