-- `enable_join_in_memory_compression` compresses the stored right-side blocks of a hash join only once
-- they grow past `join_in_memory_compression_min_bytes`.

DROP TABLE IF EXISTS jimc_mb_left;
DROP TABLE IF EXISTS jimc_mb_right;

CREATE TABLE jimc_mb_left (k UInt64) ENGINE = Memory;
CREATE TABLE jimc_mb_right (k UInt64, pad String) ENGINE = Memory;

INSERT INTO jimc_mb_left SELECT number FROM numbers(10000);
-- About 10 MB uncompressed, in many blocks.
INSERT INTO jimc_mb_right SELECT number, repeat('x', 1000) FROM numbers(10000) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000;

SET join_algorithm = 'hash', query_plan_join_swap_table = 'false',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- Below the threshold nothing is compressed.
SELECT sum(cityHash64(l.k, r.pad)) FROM jimc_mb_left AS l INNER JOIN jimc_mb_right AS r ON l.k = r.k
SETTINGS enable_join_in_memory_compression = 1, join_in_memory_compression_min_bytes = '1Gi', log_comment = '05407_under_threshold'
FORMAT Null;

-- The threshold is crossed in the middle of the build side: the blocks stored so far are compressed,
-- and so are the blocks added later. The result does not change.
SELECT (SELECT sum(cityHash64(l.k, r.pad)) FROM jimc_mb_left AS l INNER JOIN jimc_mb_right AS r ON l.k = r.k
            SETTINGS enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, r.pad)) FROM jimc_mb_left AS l INNER JOIN jimc_mb_right AS r ON l.k = r.k
            SETTINGS enable_join_in_memory_compression = 1, join_in_memory_compression_min_bytes = '4Mi')
SETTINGS log_comment = '05407_over_threshold';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['JoinInMemoryCompressedColumns'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05407_under_threshold', '05407_over_threshold') AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE jimc_mb_left;
DROP TABLE jimc_mb_right;
