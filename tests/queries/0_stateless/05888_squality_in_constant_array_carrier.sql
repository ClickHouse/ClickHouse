SELECT
    [toUInt8(1)] IN CAST([CAST([toUInt8(1)] AS Dynamic)] AS Array(Dynamic)),
    [toUInt8(1)] IN materialize(CAST([CAST([toUInt8(1)] AS Dynamic)] AS Array(Dynamic)))
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM values('a Array(UInt8)', ([1]))
WHERE a IN CAST([CAST([toUInt8(1)] AS Dynamic)] AS Array(Dynamic))
SETTINGS in_null_as_unknown = 1;

SELECT count() FROM values('a Array(UInt8)', ([1]))
WHERE a IN materialize(CAST([CAST([toUInt8(1)] AS Dynamic)] AS Array(Dynamic)))
SETTINGS in_null_as_unknown = 1;

SELECT
    [tuple(toUInt8(1))] IN CAST([CAST([tuple(toUInt8(1))] AS Dynamic)] AS Array(Dynamic)),
    [tuple(toUInt8(1))] IN materialize(CAST([CAST([tuple(toUInt8(1))] AS Dynamic)] AS Array(Dynamic)))
SETTINGS in_null_as_unknown = 1;
