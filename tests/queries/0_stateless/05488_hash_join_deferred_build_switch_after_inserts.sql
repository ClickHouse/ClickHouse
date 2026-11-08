-- A deferred build reserves a bucket for at most 4 times its even share of the distinct keys. So when the keys pack into
-- a few buckets, the projection is far below what the maps take. After its inserts the build compares what the maps hold
-- with the spill threshold, as the streaming build does after its last block, and switches to `GraceHashJoin` as well.

SET join_algorithm = 'parallel_hash';
-- With one bucket, which a small row estimate of the right side would choose, the projection is exact.
SET hash_join_layout = 'parallel';
SET collect_hash_table_stats_during_joins = 0;
-- Above the projection of the deferred build (2.1 MB) and below what both builds hold (3.7 MB).
SET max_bytes_before_external_join = 3000000;
SET max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_r;
CREATE TABLE t_r (k UInt64) ENGINE = Memory;
-- 65536 keys in one of the 256 buckets of the map, which hashes a `UInt64` key with CRC32C (on x86 with SSE 4.2 and on
-- aarch64). crc32c(v[j]) xor crc32c(0) = 1 << j, and the key of n is the XOR of v[j] for the set bits j of n. So the hashes
-- of the keys differ only in their low 16 bits, and the bucket comes from the high bits. One block, so that no check
-- between blocks switches the join first.
INSERT INTO t_r
SELECT arrayFold((acc, v, j) -> if(bitTest(number, j), bitXor(acc, v), acc),
    [2848840205, 1450689259, 2901378518, 1546878301, 3093756602, 1965130629, 3930261258, 3513415909,
     2805499707, 1268611207, 2537222414, 731485421, 1462970842, 2925941684, 1495323033, 2990646066],
    range(16), toUInt64(0))
FROM numbers(65536)
SETTINGS max_block_size = 65536, max_threads = 1, max_insert_threads = 1;

SELECT count() FROM t_r l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, log_comment = '05488_deferred';
SELECT count() FROM t_r l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, log_comment = '05488_streaming';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinDeferredBuilds'], ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05488_deferred', '05488_streaming')
ORDER BY log_comment;

DROP TABLE t_r;
