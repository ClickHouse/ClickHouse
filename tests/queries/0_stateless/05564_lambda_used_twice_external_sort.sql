-- A lambda used in two clauses (or in a sort key and the projection) must not reach the sort as a `Function` column,
-- which cannot be written to a temporary file nor turned into a virtual row.

DROP TABLE IF EXISTS t_lambda_sort;
CREATE TABLE t_lambda_sort (c1 String, c2 DateTime64(6), c3 String, arr Array(String)) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_lambda_sort
SELECT leftPad(toString(number), 8, '0'), toDateTime64('2026-08-01 00:00:00', 6) + number, 'v', if(number % 200 = 0, ['2', 'x'], ['zz'])
FROM numbers(10000);

-- Every sort spills to disk.
SET max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0;
-- Few temporary files per sort.
SET max_block_size = 1000;
-- The projection is computed above the sort.
SET query_plan_execute_functions_after_sorting = 1;

SELECT c1, arrayFirst(x -> x = '2' AND c3 != '', arr) AS f FROM t_lambda_sort
WHERE arrayExists(x -> x = '2' AND c3 != '', arr) ORDER BY c2 DESC LIMIT 2;

SELECT c1, arrayFirst(x -> x IN (SELECT '2') AND c3 != '', arr) AS f FROM t_lambda_sort
WHERE arrayExists(x -> x IN (SELECT '2') AND c3 != '', arr) ORDER BY c2 DESC LIMIT 2;

SELECT c1, arrayFirst(x -> arrayExists(y -> y = '2' AND c3 != '', [x]), arr) AS f FROM t_lambda_sort
WHERE arrayExists(x -> arrayExists(y -> y = '2' AND c3 != '', [x]), arr) ORDER BY c2 DESC LIMIT 2;

SELECT c1, arrayFirst(x -> x = '2' AND c3 != '', arr) AS f, row_number() OVER (ORDER BY c2 DESC) AS rn FROM t_lambda_sort
WHERE arrayExists(x -> x = '2' AND c3 != '', arr) ORDER BY rn LIMIT 2;

SELECT c1, arrayFirst(x -> x = '2' AND c3 != '', arr) AS f FROM t_lambda_sort
WHERE arrayExists(x -> x = '2' AND c3 != '', arr) ORDER BY c2 DESC LIMIT 1 BY f LIMIT 2;

SELECT c1, arrayCount(x -> x = '2' AND c3 != '', arr) AS n FROM t_lambda_sort
ORDER BY arrayFirst(x -> x = '2' AND c3 != '', arr) DESC, c2 DESC LIMIT 2;

SELECT count() FROM (
    EXPLAIN header = 1 SELECT c1, arrayFirst(x -> x = '2' AND c3 != '', arr) AS f FROM t_lambda_sort
    WHERE arrayExists(x -> x = '2' AND c3 != '', arr) ORDER BY c2 DESC LIMIT 2)
WHERE explain LIKE '% Function(%';

SELECT count() FROM (
    EXPLAIN header = 1 SELECT c1, arrayCount(x -> x = '2' AND c3 != '', arr) AS n FROM t_lambda_sort
    ORDER BY arrayFirst(x -> x = '2' AND c3 != '', arr) DESC, c2 DESC LIMIT 2)
WHERE explain LIKE '% Function(%';

DROP TABLE t_lambda_sort;

-- https://github.com/ClickHouse/ClickHouse/issues/127748: the virtual row of an in-order read.
DROP TABLE IF EXISTS t_lambda_virtual_row;
CREATE TABLE t_lambda_virtual_row (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_lambda_virtual_row SELECT number, ['a', 'b', 'c'][number % 3 + 1] FROM numbers(20);

SELECT DISTINCT k, arrayFirst(x -> x = s, ['a', 'b']) AS f FROM t_lambda_virtual_row
WHERE arrayExists(x -> x = s, ['a', 'b']) ORDER BY k LIMIT 5
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, max_bytes_before_external_sort = 0;

DROP TABLE t_lambda_virtual_row;

-- A lambda applied to an ARRAY JOINed column is built above the join.
DROP TABLE IF EXISTS t_lambda_array_join;
CREATE TABLE t_lambda_array_join (n UInt64, arrays Array(Array(UInt64))) ENGINE = Memory;
SELECT count() FROM (EXPLAIN header = 1 SELECT n, arrayMap(x -> x + n, a) FROM t_lambda_array_join ARRAY JOIN arrays AS a)
WHERE explain LIKE '% Function(%';
SELECT count() FROM (EXPLAIN header = 1 SELECT arrayMap(x -> x + n * 2, [n]) AS m, arrayMap(x -> x + n * 2, a) AS k
    FROM t_lambda_array_join ARRAY JOIN arrays AS a)
WHERE explain LIKE '% Function(%';
DROP TABLE t_lambda_array_join;
