-- A join with several `OR` clauses inserts each right block once per clause. The in-memory compression
-- started after an earlier clause may compress the block being inserted; if the block is then dropped
-- (a key-only `LEFT SEMI` join keeps no right block), its accounted size must be the compressed one.
-- Debug builds check this accounting in `HashJoin::doDebugAsserts`.

DROP TABLE IF EXISTS t_left_or;
DROP TABLE IF EXISTS t_right_or;

CREATE TABLE t_left_or (a UInt64, b UInt64, val String) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_left_or VALUES (1, 10, 'L1'), (2, 20, 'L2'), (3, 30, 'L3'), (4, 40, 'L4');

CREATE TABLE t_right_or (a UInt64, b UInt64, val String) ENGINE = MergeTree() ORDER BY a;
INSERT INTO t_right_or VALUES (1, 99, 'Ra1'), (99, 20, 'Rb2'), (3, 30, 'Rab3'), (99, 99, 'Rnone');

SET enable_join_in_memory_compression = 1, join_in_memory_compression_min_bytes = 0;

SELECT l.a, l.b, l.val FROM t_left_or l LEFT SEMI JOIN t_right_or r ON l.a = r.a OR l.b = r.b ORDER BY l.a
SETTINGS join_algorithm = 'hash';

-- `grace_hash` rehashing feeds the rows that stay in the in-memory bucket into the replacement join
-- one block at a time.
SELECT count(), sum(l.number), sum(r.number)
FROM numbers(100000) AS l
INNER JOIN (SELECT number FROM numbers(100000)) AS r ON l.number = r.number
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 1, max_bytes_before_external_join = 100000, max_block_size = 1000;

DROP TABLE t_left_or;
DROP TABLE t_right_or;
