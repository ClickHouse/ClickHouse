-- A filter that is always true must keep the rows of its arrayJoin.

DROP TABLE IF EXISTS t_const_filter_array_join;
CREATE TABLE t_const_filter_array_join (id UInt64, tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_const_filter_array_join VALUES (1, ['a', 'b']), (2, ['c']);

SELECT count() FROM numbers(10) WHERE arrayJoin([0, 1]) OR 1;
SELECT count() FROM numbers(10) WHERE arrayJoin([0, 1]) IS NOT NULL;
SELECT count() FROM numbers(10) WHERE arrayJoin(range(number % 3)) OR 1;
SELECT count() FROM numbers(1) WHERE NOT (toUInt64(arrayJoin([1, 2, 3])) IN CAST([], 'Array(UInt64)'));
SELECT count() FROM t_const_filter_array_join WHERE arrayJoin(tags) NOT IN ();
SELECT number, a FROM numbers(2) WHERE (arrayJoin([10, 20]) AS a) OR 1 ORDER BY number, a;
SELECT count() FROM (SELECT arrayJoin([0, 1]) AS a FROM numbers(10) WHERE a OR 1);
SELECT count() FROM (SELECT arrayJoin([0, 1]) OR 1 AS x FROM numbers(10) WHERE x);
SELECT groupArray(number) FROM (SELECT number FROM numbers(10) WHERE arrayJoin(range(number % 3)) OR 1 ORDER BY number LIMIT 5);
SELECT count() FROM (SELECT number FROM numbers(3)) AS l JOIN (SELECT number FROM numbers(3)) AS r USING (number) WHERE arrayJoin([0, 1]) OR 1;
SELECT count() FROM remote('127.0.0.{1,2}', system.one) WHERE arrayJoin([0, 1]) OR 1;
SELECT count() FROM (SELECT number % 2 AS k FROM numbers(10) GROUP BY k HAVING arrayJoin([0, 1]) OR 1);
SELECT count() FROM (SELECT number, row_number() OVER () AS rn FROM numbers(10) QUALIFY arrayJoin([0, 1]) OR 1);
SELECT count() FROM (SELECT number FROM numbers(10) QUALIFY arrayJoin([0, 1]) OR 1);
SELECT count() FROM numbers(10) WHERE arrayJoin([0, 1]) OR 1 SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE t_const_filter_array_join;
