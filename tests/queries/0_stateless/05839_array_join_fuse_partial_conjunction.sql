-- a conjunction over ARRAY JOIN elements with a conjunct that must stay above: the other conjuncts go into the step

-- a serialized plan does not fuse a filter into ARRAY JOIN
SET serialize_query_plan = 0;
SET query_plan_fuse_filter_into_array_join = 1;

DROP TABLE IF EXISTS t_aj_partial;
CREATE TABLE t_aj_partial (id UInt64, A Array(String), B Array(String)) ENGINE = MergeTree ORDER BY id;

-- the arrays have 0 to 4 and 0 to 3 elements
INSERT INTO t_aj_partial SELECT number,
    arrayMap(x -> concat('a', toString(x)), range(number % 5)),
    arrayMap(x -> concat('b', toString(x)), range(number % 4))
FROM numbers(60);

SELECT '-- the step gets the element conjunct, the filter above keeps only the conjunct with rand';
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND rand() >= 0)
WHERE explain LIKE '%Element filter column%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND rand() >= 0)
WHERE explain LIKE '%Filter column:%rand%' AND explain NOT LIKE '%a3%';
SELECT count() FROM (EXPLAIN SELECT arrayJoin(A) AS a FROM t_aj_partial WHERE a = 'a3' AND rand() >= 0)
WHERE explain LIKE '%Element filter column%';

SELECT '-- a function that is not cheap but cannot throw goes into the step too';
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE startsWith(a, 'a3') AND rand() >= 0)
WHERE explain LIKE '%Element filter column%';
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE rand() >= 0 AND endsWith(a, '3'))
WHERE explain LIKE '%Element filter column%';
SELECT groupArray(id) FROM (SELECT id FROM t_aj_partial ARRAY JOIN A AS a WHERE startsWith(a, 'a3') AND (rand() % 1 + id) % 2 = 0 ORDER BY id);

SELECT '-- two arrays: each step gets its conjunct';
SELECT count() FROM (EXPLAIN SELECT a, b FROM t_aj_partial ARRAY JOIN A AS a ARRAY JOIN B AS b WHERE a = 'a3' AND b = 'b1' AND rand() >= 0)
WHERE explain LIKE '%Element filter column%';

SELECT '-- the conjunct above still filters: it is the same as id % 2 = 0';
SELECT groupArray(id) FROM (SELECT id FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND (rand() % 1 + id) % 2 = 0 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND (rand() % 1 + id) % 2 = 0 ORDER BY id)
SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT groupArray((id, a, b)) FROM (SELECT id, a, b FROM t_aj_partial LEFT ARRAY JOIN A AS a LEFT ARRAY JOIN B AS b
    WHERE a = '' AND b != 'b0' AND (rand() % 1 + id) % 2 = 0 ORDER BY id, b);
SELECT groupArray((id, a, b)) FROM (SELECT id, a, b FROM t_aj_partial LEFT ARRAY JOIN A AS a LEFT ARRAY JOIN B AS b
    WHERE a = '' AND b != 'b0' AND (rand() % 1 + id) % 2 = 0 ORDER BY id, b)
SETTINGS query_plan_fuse_filter_into_array_join = 0;

SELECT '-- the filter column is in the result';
SELECT groupArray((id, f)) FROM (SELECT id, a = 'a3' AND (rand() % 1 + id) % 2 = 0 AS f FROM t_aj_partial ARRAY JOIN A AS a WHERE f ORDER BY id);

SELECT '-- a stateful function above must see all the rows: no fusion';
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND rowNumberInAllBlocks() < 40)
WHERE explain LIKE '%Element filter column%';
SELECT count() FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND rowNumberInAllBlocks() < 40 SETTINGS max_threads = 1;
SELECT count() FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND rowNumberInAllBlocks() < 40
SETTINGS max_threads = 1, query_plan_fuse_filter_into_array_join = 0;

SELECT '-- the conjuncts that can move do not read an element: no fusion';
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE id % 2 = 0 AND rand() % 1 + length(a) = 2
    SETTINGS query_plan_filter_push_down = 0)
WHERE explain LIKE '%Element filter column%';
SELECT count() FROM t_aj_partial ARRAY JOIN A AS a WHERE id % 2 = 0 AND rand() % 1 + length(a) = 2 SETTINGS query_plan_filter_push_down = 0;

SELECT '-- the step does not read a column that only the conjunct above reads';
SELECT extractAll(extract(explain, '"Element Filter Expression": \\{\\s*"Inputs": \\[([^\\]]*)\\]'), '"Type": "([^"]*)"')
FROM (EXPLAIN json = 1, actions = 1 SELECT a FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND rand(id) >= 0);
SELECT groupArray(id) FROM (SELECT id FROM t_aj_partial ARRAY JOIN A AS a WHERE a = 'a3' AND (rand(id) % 1 + id) % 2 = 0 ORDER BY id);

DROP TABLE t_aj_partial;

SELECT '-- a conjunct that can throw keeps its guard above: no fusion';
DROP TABLE IF EXISTS t_aj_partial_throw;
CREATE TABLE t_aj_partial_throw (A Array(Int64)) ENGINE = Memory;
INSERT INTO t_aj_partial_throw VALUES ([0, 1]), ([2, 0]);
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial_throw ARRAY JOIN A AS a WHERE (a != 0 OR rand() < 0) AND intDiv(2, a) > 0)
WHERE explain LIKE '%Element filter column%';
SELECT a FROM t_aj_partial_throw ARRAY JOIN A AS a WHERE (a != 0 OR rand() < 0) AND intDiv(2, a) > 0
ORDER BY a SETTINGS short_circuit_function_evaluation = 'enable';

SELECT '-- a function with side effects above must see all the rows: no fusion';
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial_throw ARRAY JOIN A AS a WHERE sleepEachRow(toFloat64(randConstant(a) % 1) + 0.01) + rand() % 1 = 0 AND a = 1)
WHERE explain LIKE '%Element filter column%';
-- sleepEachRow asks for 40 ms for the four rows of the block, and the limit is 20 ms
SELECT count() FROM t_aj_partial_throw ARRAY JOIN A AS a WHERE sleepEachRow(toFloat64(randConstant(a) % 1) + 0.01) + rand() % 1 = 0 AND a = 1
SETTINGS function_sleep_max_microseconds_per_block = 20000, max_block_size = 65505; -- { serverError TOO_SLOW }
SELECT count() FROM (EXPLAIN SELECT a FROM t_aj_partial_throw ARRAY JOIN A AS a WHERE a = 1 AND arrayExists(x -> sleepEachRow(toFloat64(randConstant(x) % 1) + 0.001) + rand() % 1 = 0, [a]))
WHERE explain LIKE '%Element filter column%';
DROP TABLE t_aj_partial_throw;
