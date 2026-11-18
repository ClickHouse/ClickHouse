SELECT
    x IN (materialize(CAST(toUInt8(1) AS Dynamic)), materialize(CAST(toUInt8(2) AS Dynamic))),
    x NOT IN (materialize(CAST(toUInt8(1) AS Dynamic)), materialize(CAST(toUInt8(2) AS Dynamic)))
FROM values('x Nullable(UInt8)', (NULL), (1), (3))
SETTINGS in_null_as_unknown = 1;

SELECT count()
FROM values('x Nullable(UInt8)', (NULL), (1), (3))
WHERE x NOT IN (materialize(CAST(toUInt8(1) AS Dynamic)), materialize(CAST(toUInt8(2) AS Dynamic)))
SETTINGS in_null_as_unknown = 1;
