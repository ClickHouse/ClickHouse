-- The full sorting merge join finishes as soon as a side it cannot do without is empty: the left side of a left or inner join,
-- the right side of a right or inner join. The other side is not waited for, so its sort over an endless source ends with the join
-- instead of running until `max_execution_time`.

SET join_algorithm = 'full_sorting_merge';
SET max_block_size = 100;
SET max_threads = 2;
SET max_execution_time = 120;

DROP TABLE IF EXISTS t_empty;
CREATE TABLE t_empty (k UInt64) ENGINE = MergeTree ORDER BY k;

SELECT '-- inner join, empty left side';
SELECT count() FROM t_empty INNER JOIN (SELECT number AS k FROM system.numbers WHERE sleepEachRow(0.001) = 0) AS r USING (k);

SELECT '-- inner join, empty right side';
SELECT count() FROM (SELECT number AS k FROM system.numbers WHERE sleepEachRow(0.001) = 0) AS l INNER JOIN t_empty USING (k);

SELECT '-- left join, empty left side';
SELECT count() FROM t_empty LEFT JOIN (SELECT number AS k FROM system.numbers WHERE sleepEachRow(0.001) = 0) AS r USING (k);

SELECT '-- right join, empty right side';
SELECT count() FROM (SELECT number AS k FROM system.numbers WHERE sleepEachRow(0.001) = 0) AS l RIGHT JOIN t_empty USING (k);

SELECT '-- a left join keeps the left rows when the right side is empty';
SELECT count() FROM (SELECT number AS k FROM numbers(10)) AS l LEFT JOIN t_empty USING (k);

SELECT '-- a right join keeps the right rows when the left side is empty';
SELECT count() FROM t_empty RIGHT JOIN (SELECT number AS k FROM numbers(10)) AS r USING (k);

SELECT '-- a full join keeps the rows of both sides';
SELECT count() FROM t_empty FULL JOIN (SELECT number AS k FROM numbers(10)) AS r USING (k);
SELECT count() FROM (SELECT number AS k FROM numbers(10)) AS l FULL JOIN t_empty USING (k);

DROP TABLE t_empty;
