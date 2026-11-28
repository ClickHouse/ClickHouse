-- `x = ALL (subquery)` with a non-empty right-hand side gives the same results as the former
-- `x IN (SELECT singleValueOrNull(*) FROM subquery)`: a NULL `x` follows `transform_null_in`,
-- and the result is not nullable for a non-nullable `x`.

SELECT 'transform_null_in = 0';
SELECT NULL = ALL (SELECT NULL), NULL = ALL (SELECT 1), 1 = ALL (SELECT NULL), NULL = ALL (SELECT 1 WHERE 0) SETTINGS transform_null_in = 0;
SELECT n, n = ALL (SELECT 1), n = ALL (SELECT NULL) FROM (SELECT arrayJoin([1, 2, NULL]) AS n) ORDER BY n SETTINGS transform_null_in = 0;

SELECT 'transform_null_in = 1';
SELECT NULL = ALL (SELECT NULL), NULL = ALL (SELECT 1), 1 = ALL (SELECT NULL), NULL = ALL (SELECT 1 WHERE 0) SETTINGS transform_null_in = 1;
SELECT n, n = ALL (SELECT 1), n = ALL (SELECT NULL) FROM (SELECT arrayJoin([1, 2, NULL]) AS n) ORDER BY n SETTINGS transform_null_in = 1;

-- Tuples are compared as by `IN`, so a tuple containing NULL is equal to itself, and a NULL left-hand side
-- compared with a tuple is decided by `transform_null_in`.
SELECT 'tuples';
SELECT (1, NULL) = ALL (SELECT (1, NULL)), (1, NULL) = ALL (SELECT (2, NULL)), (1, NULL) = ALL (SELECT (1, NULL) WHERE 0), NULL = ALL (SELECT (1, 2)), (1, (2, NULL)) = ALL (SELECT (1, (2, NULL))) SETTINGS transform_null_in = 0;
SELECT (1, NULL) = ALL (SELECT (1, NULL)), (1, NULL) = ALL (SELECT (2, NULL)), (1, NULL) = ALL (SELECT (1, NULL) WHERE 0), NULL = ALL (SELECT (1, 2)), (1, (2, NULL)) = ALL (SELECT (1, (2, NULL))) SETTINGS transform_null_in = 1;
SELECT number, (number, NULL) = ALL (SELECT (1, NULL)), (number, toNullable(number)) = ALL (SELECT (1, toNullable(1))), (number, CAST(NULL AS Nullable(UInt8))) = ALL (SELECT (1, CAST(NULL AS Nullable(UInt8)))) FROM numbers(3) ORDER BY number;

SELECT 'result type';
SELECT toTypeName(number = ALL (SELECT 1)), toTypeName(number = ALL (SELECT NULL)), toTypeName(toNullable(number) = ALL (SELECT 1)) FROM numbers(1);

DROP TABLE IF EXISTS t_equals_all_key;
CREATE TABLE t_equals_all_key ENGINE = MergeTree ORDER BY flag AS SELECT number = ALL (SELECT 1) AS flag FROM numbers(2);
SELECT flag FROM t_equals_all_key ORDER BY flag;
DROP TABLE t_equals_all_key;

-- The left-hand side is evaluated once per row.
SELECT 'single evaluation of the left-hand side';
SELECT arrayJoin([1, 2]) = ALL (SELECT 1);
SELECT count() FROM numbers(1000) WHERE (rand() % 2 = ALL (SELECT 1)) != ((rand() % 2) = 1);
