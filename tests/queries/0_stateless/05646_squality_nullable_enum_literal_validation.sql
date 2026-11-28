SET in_null_as_unknown = 1;
SET validate_enum_literals_in_operators = 0;

SELECT CAST('a' AS Enum8('a' = 1)) IN ('b');
SELECT CAST('a' AS Enum8('a' = 1)) NOT IN ('b');
SELECT CAST('a' AS Enum8('a' = 1)) IN ('b', NULL);
SELECT CAST('a' AS Enum8('a' = 1)) IN ('a', 'b', NULL);
SELECT CAST('a' AS Enum8('a' = 1)) IN (materialize('b'), NULL);
SELECT e IN (s, NULL)
FROM (SELECT CAST('a' AS Enum8('a' = 1)) AS e, materialize('b') AS s);
