SET in_null_as_unknown = 1;

SELECT '1' IN (toFloat64(1), NULL), '1' IN (materialize(toFloat64(1)), NULL);
SELECT CAST('a' AS Enum8('a' = 1)) IN ('1', NULL),
    CAST('a' AS Enum8('a' = 1)) IN (materialize('1'), NULL);
SELECT '2' IN (toFloat64(1), NULL), '2' IN (materialize(toFloat64(1)), NULL);
SELECT CAST('a' AS Enum8('a' = 1)) IN ('2', NULL),
    CAST('a' AS Enum8('a' = 1)) IN (materialize('2'), NULL);

SELECT count()
FROM numbers(1)
WHERE '1' IN (materialize(toFloat64(1)), NULL);
SELECT count()
FROM numbers(1)
WHERE CAST('a' AS Enum8('a' = 1)) IN (materialize('1'), NULL);

SELECT '1' IN [materialize(toFloat64(1)), NULL],
    '2' IN [materialize(toFloat64(1)), NULL];
SELECT CAST('a' AS Enum8('a' = 1)) IN [materialize('1'), NULL],
    CAST('a' AS Enum8('a' = 1)) IN [materialize('2'), NULL];

SET allow_experimental_analyzer = 0;
SELECT '1' IN (toFloat64(1), NULL), '1' IN (materialize(toFloat64(1)), NULL);
SELECT CAST('a' AS Enum8('a' = 1)) IN ('1', NULL),
    CAST('a' AS Enum8('a' = 1)) IN (materialize('1'), NULL);
