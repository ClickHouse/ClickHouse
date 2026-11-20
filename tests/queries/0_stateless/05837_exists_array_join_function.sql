-- Random settings limits: execute_exists_as_scalar_subquery=(1, 1)
-- `EXISTS` must see the rows an `arrayJoin` in its subquery produces.

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt64) ENGINE = Memory;
INSERT INTO t VALUES (1);

SELECT EXISTS (SELECT arrayJoin(CAST([], 'Array(UInt8)')) FROM t);
SELECT EXISTS (SELECT arrayJoin([1, 2]) FROM t);
SELECT EXISTS (SELECT arrayJoin(groupArray(k)) FROM t WHERE k = 5);
SELECT EXISTS (SELECT arrayJoin(groupArray(k)) FROM t);
SELECT EXISTS (SELECT arrayJoin([count()]) FROM t WHERE k = 5);

SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT l.number, arrayJoin(CAST([], 'Array(UInt8)')) FROM t);
SELECT count() FROM numbers(3) AS l WHERE NOT EXISTS (SELECT l.number, arrayJoin(CAST([], 'Array(UInt8)')) FROM t);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT l.number, arrayJoin([1, 2]) FROM t);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT arrayJoin(if(l.number = 1, [1], CAST([], 'Array(UInt8)'))) FROM t);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT arrayJoin(CAST([], 'Array(UInt8)')) FROM t WHERE t.k = l.number);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT arrayJoin([1, 2]) FROM t WHERE t.k = l.number);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT arrayJoin(if(t.k = 1, CAST([], 'Array(UInt8)'), [1])) FROM t WHERE t.k = l.number GROUP BY t.k);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT arrayJoin([count()]) FROM t WHERE t.k = l.number);
SELECT count() FROM numbers(3) AS l WHERE NOT EXISTS (SELECT arrayJoin([count()]) FROM t WHERE t.k = l.number);
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT arrayJoin([sumState(toNullable(t.k))]) FROM t WHERE t.k = l.number);
SELECT count() FROM numbers(5) AS l WHERE EXISTS (SELECT arrayJoin([t.number]) FROM numbers(5) AS t WHERE t.number = l.number GROUP BY t.number SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any', max_block_size = 1) SETTINGS max_threads = 1;
SELECT count() FROM numbers(5) AS l WHERE EXISTS (SELECT arrayJoin([t.number]) FROM numbers(5) AS t WHERE t.number = l.number GROUP BY t.number SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break', max_block_size = 1) SETTINGS max_threads = 1;
SELECT count() FROM numbers(5) AS l WHERE EXISTS (SELECT arrayJoin([t.number]) FROM numbers(5) AS t WHERE t.number = l.number GROUP BY t.number SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'throw', max_block_size = 1) SETTINGS max_threads = 1;
SELECT count() FROM numbers(5) AS l WHERE NOT EXISTS (SELECT arrayJoin([t.number]) FROM numbers(5) AS t WHERE t.number = l.number GROUP BY t.number SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any', max_block_size = 1) SETTINGS max_threads = 1;
SELECT count() FROM numbers(5) AS l WHERE EXISTS (SELECT k FROM (SELECT arrayJoin(if(t.number = 2, [], [t.number])) AS k FROM numbers(5) AS t WHERE t.number = l.number) GROUP BY k SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any', max_block_size = 1) SETTINGS max_threads = 1;
SELECT count() FROM numbers(3) AS l WHERE EXISTS (SELECT l.number, a FROM t ARRAY JOIN CAST([], 'Array(UInt8)') AS a);

DROP TABLE t;
