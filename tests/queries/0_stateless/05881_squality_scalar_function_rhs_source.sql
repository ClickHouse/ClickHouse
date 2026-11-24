SET in_null_as_unknown = 1;

SELECT '1' IN toFloat64(1), '1' IN materialize(toFloat64(1));
SELECT CAST('a' AS Enum8('a' = 1)) IN '1',
    CAST('a' AS Enum8('a' = 1)) IN materialize('1');

SELECT '1' NOT IN toFloat64(1), '1' NOT IN materialize(toFloat64(1));
SELECT CAST('a' AS Enum8('a' = 1)) NOT IN '1',
    CAST('a' AS Enum8('a' = 1)) NOT IN materialize('1');
