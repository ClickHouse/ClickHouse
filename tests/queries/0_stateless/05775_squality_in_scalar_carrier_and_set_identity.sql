SELECT
    true IN (CAST(toUInt8(2) AS Dynamic), NULL),
    true IN (materialize(CAST(toUInt8(2) AS Dynamic)), NULL),
    true IN materialize([CAST(toUInt8(2) AS Dynamic), NULL])
SETTINGS in_null_as_unknown = 1;

SELECT
    true IN (CAST(toUInt8(2) AS Variant(UInt8, String)), NULL),
    true IN (materialize(CAST(toUInt8(2) AS Variant(UInt8, String))), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    '1' IN (CAST(toUInt8(1) AS Dynamic), NULL),
    '1' IN (materialize(CAST(toUInt8(1) AS Dynamic)), NULL),
    '1' IN materialize([CAST(toUInt8(1) AS Dynamic), NULL])
SETTINGS in_null_as_unknown = 1;

SELECT
    countIf(s IN (CAST(toUInt8(1) AS Dynamic), NULL)),
    countIf(s IN (materialize(CAST(toUInt8(1) AS Dynamic)), NULL))
FROM (SELECT toString(number) AS s FROM numbers(2))
SETTINGS in_null_as_unknown = 1;

SELECT
    materialize(true) IN (2, NULL),
    materialize(toUInt8(2)) IN (2, NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    materialize(toUInt8(2)) IN (2, NULL),
    materialize(true) IN (2, NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    materialize(true) IN (2),
    materialize(toUInt8(2)) IN (2);
