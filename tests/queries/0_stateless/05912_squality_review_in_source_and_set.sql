SET in_null_as_unknown = 1;
SET validate_enum_literals_in_operators = 0;

SELECT isNull(CAST('a' AS Enum8('a' = 1, 'b' = 2)) IN
    ('unknown', CAST(toUInt8(2) AS Dynamic), NULL));
SELECT isNull(CAST('a' AS Enum8('a' = 1, 'b' = 2)) IN
    (materialize('unknown'), materialize(CAST(toUInt8(2) AS Dynamic)), NULL));

SELECT isNull([toUInt8(1)] IN (SELECT CAST(NULL AS Nullable(UInt8))));
SELECT isNull([toUInt8(1)] NOT IN (SELECT CAST(NULL AS Nullable(UInt8))));

SET in_null_as_unknown = 0;
WITH [CAST([toUInt8(1)] AS Variant(Array(UInt8), UInt8))] AS x
SELECT x IN x;
SELECT count()
FROM numbers(1)
WHERE [CAST([toUInt8(1)] AS Variant(Array(UInt8), UInt8))]
    IN [CAST([toUInt8(1)] AS Variant(Array(UInt8), UInt8))];
