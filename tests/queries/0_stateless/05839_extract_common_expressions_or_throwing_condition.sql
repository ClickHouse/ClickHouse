-- A common condition that can throw stays behind the conditions each disjunct evaluates before it, unless every disjunct starts with it.

SET optimize_extract_common_expressions = 1, short_circuit_function_evaluation = 'enable';

SELECT count() FROM (SELECT number % 10 AS x FROM numbers(10))
WHERE (x != 0 AND intDiv(10, x) > 0) OR (x > 5 AND intDiv(10, x) > 0);

SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y FROM numbers(10))
WHERE (x != 0 AND intDiv(10, x) > 0) OR (intDiv(10, x) > 0 AND y > 5)
SETTINGS short_circuit_function_evaluation_reorder_arguments = 1;

SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y, toString(number) AS s FROM numbers(10))
WHERE (s != 'zz' AND x != 0 AND intDiv(10, x) > 0) OR (y > 5 AND intDiv(10, x) > 0 AND s != 'zz');

SELECT count() FROM (SELECT number % 7 AS y, toString(number) AS s FROM numbers(10))
WHERE (y > 5 AND intDiv(if(s LIKE '%1%', toInt8(-128), toInt8(1)), -1) != 0)
    OR (y = 3 AND intDiv(if(s LIKE '%1%', toInt8(-128), toInt8(1)), -1) != 0);

SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y, toString(number) AS s FROM numbers(10))
WHERE s != 'zz' AND ((x != 0 AND intDiv(10, x) > 0) OR (y > 5 AND x != 0 AND intDiv(10, x) > 0))
SETTINGS short_circuit_function_evaluation_reorder_arguments = 0;

SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y, toString(number) AS s FROM numbers(10))
WHERE s != 'zz' AND ((y > 5 AND x != 0) OR (x != 0 AND intDiv(10, x) > 1))
SETTINGS short_circuit_function_evaluation_reorder_arguments = 0;

SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y FROM numbers(10))
WHERE (x != 0 AND y < 6 AND intDiv(10, x) > 0) OR (y < 6 AND x != 0 AND intDiv(10, x) > 0);

SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y, toString(number) AS s FROM numbers(10))
WHERE s != 'zz' AND ((x != 0 AND y < 6 AND intDiv(10, x) > 0) OR (y < 6 AND x != 0 AND intDiv(10, x) > 0));

SELECT trim(line) FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y, toString(number) AS s FROM numbers(10))
    WHERE (s != 'zz' AND x != 0 AND intDiv(10, x) > 0) OR (y > 5 AND intDiv(10, x) > 0 AND s != 'zz')
)
ARRAY JOIN splitByChar('\n', explain) AS line WHERE line LIKE 'WHERE%';

SELECT trim(line) FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT count() FROM (SELECT number % 7 AS y, toString(number) AS s FROM numbers(10))
    WHERE (y > 5 AND s LIKE '%1%') OR (y < 2 AND s LIKE '%1%')
)
ARRAY JOIN splitByChar('\n', explain) AS line WHERE line LIKE 'WHERE%';

SELECT trim(line) FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y FROM numbers(10))
    WHERE (x != 0 AND intDiv(10, x) > 0 AND y > 2) OR (x != 0 AND intDiv(10, x) > 0 AND y < 2)
)
ARRAY JOIN splitByChar('\n', explain) AS line WHERE line LIKE 'WHERE%';

SELECT trim(line) FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT count() FROM (SELECT number % 10 AS x, number % 7 AS y FROM numbers(10))
    WHERE (x != 0 AND y < 6 AND intDiv(10, x) > 0) OR (y < 6 AND x != 0 AND intDiv(10, x) > 0)
)
ARRAY JOIN splitByChar('\n', explain) AS line WHERE line LIKE 'WHERE%';

SELECT trim(line) FROM
(
    EXPLAIN SYNTAX run_query_tree_passes = 1
    SELECT count() FROM (SELECT number % 7 AS y, CAST((number % 2, toString(number)), 'Tuple(a UInt8, s String)') AS t FROM numbers(10))
    WHERE (y > 5 AND t.a = 0 AND nullIf(t.s, '') IS NULL) OR (y < 2 AND t.a = 0 AND nullIf(t.s, '') IS NULL)
)
ARRAY JOIN splitByChar('\n', explain) AS line WHERE line LIKE 'WHERE%';
