SET in_null_as_unknown = 1;

WITH CAST('a' AS Enum8('a' = 1)) AS e
SELECT
    isNull(e IN (materialize(toFloat64(1.5)), NULL)),
    isNull(e IN (materialize(toFloat64(1.5)),
        materialize(CAST(toUInt8(2) AS Dynamic)), NULL));

SELECT count()
FROM numbers(1)
WHERE CAST('a' AS Enum8('a' = 1)) IN
    (materialize(toFloat64(1.5)), materialize(CAST(toUInt8(2) AS Dynamic)), NULL);
