SELECT
    map('a', toUInt8(1)) IN CAST(map('a', toUInt8(1)) AS Dynamic),
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Dynamic)),
    map('a', toUInt8(1)) IN (materialize(CAST(map('a', toUInt8(1)) AS Dynamic)), NULL),
    map('a', toUInt8(1)) NOT IN (materialize(CAST(map('a', toUInt8(1)) AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Variant(Map(String, UInt8), String))),
    map('a', toUInt8(1)) IN materialize(CAST('other' AS Variant(Map(String, UInt8), String))),
    map('a', toUInt8(1)) IN (materialize(CAST('other' AS Variant(Map(String, UInt8), String))), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    map('a', toUInt8(1)) IN [CAST(map('a', toUInt8(1)) AS Dynamic)],
    map('a', toUInt8(1)) IN [CAST('other' AS Dynamic)],
    [toUInt8(1)] IN [CAST([toUInt8(1)] AS Dynamic)]
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM numbers(1)
WHERE map('a', toUInt8(1)) IN materialize(CAST(map('a', toUInt8(1)) AS Dynamic))
SETTINGS in_null_as_unknown = 1;
