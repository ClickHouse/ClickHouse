-- 24 hash joins with an empty right side and the parallel layout. A bucket of the hash table gets its cells at the first insert
-- of its slot, so an empty build allocates almost nothing.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (k String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_right (k String, v Float64) ENGINE = MergeTree ORDER BY k;

SELECT count() FROM t_left AS l
LEFT JOIN t_right AS r1 ON l.k = r1.k
LEFT JOIN t_right AS r2 ON l.k = r2.k
LEFT JOIN t_right AS r3 ON l.k = r3.k
LEFT JOIN t_right AS r4 ON l.k = r4.k
LEFT JOIN t_right AS r5 ON l.k = r5.k
LEFT JOIN t_right AS r6 ON l.k = r6.k
LEFT JOIN t_right AS r7 ON l.k = r7.k
LEFT JOIN t_right AS r8 ON l.k = r8.k
LEFT JOIN t_right AS r9 ON l.k = r9.k
LEFT JOIN t_right AS r10 ON l.k = r10.k
LEFT JOIN t_right AS r11 ON l.k = r11.k
LEFT JOIN t_right AS r12 ON l.k = r12.k
LEFT JOIN t_right AS r13 ON l.k = r13.k
LEFT JOIN t_right AS r14 ON l.k = r14.k
LEFT JOIN t_right AS r15 ON l.k = r15.k
LEFT JOIN t_right AS r16 ON l.k = r16.k
LEFT JOIN t_right AS r17 ON l.k = r17.k
LEFT JOIN t_right AS r18 ON l.k = r18.k
LEFT JOIN t_right AS r19 ON l.k = r19.k
LEFT JOIN t_right AS r20 ON l.k = r20.k
LEFT JOIN t_right AS r21 ON l.k = r21.k
LEFT JOIN t_right AS r22 ON l.k = r22.k
LEFT JOIN t_right AS r23 ON l.k = r23.k
LEFT JOIN t_right AS r24 ON l.k = r24.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_threads = 4, max_memory_usage = '20Mi';

-- A key in a bucket that no row was inserted into is a miss.
INSERT INTO t_left SELECT toString(number) FROM numbers(1000);
INSERT INTO t_right VALUES ('1', 1), ('2', 2), ('x', 3);

SELECT count(), countIf(r.v = 1), countIf(r.v = 2) FROM t_left AS l LEFT JOIN t_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_threads = 8;

SELECT count(), countIf(l.k = ''), countIf(r.v = 2) FROM t_left AS l FULL JOIN t_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', hash_join_layout = 'parallel', max_threads = 8, join_use_nulls = 0;

DROP TABLE t_left;
DROP TABLE t_right;
