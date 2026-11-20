SET in_null_as_unknown = 1;

SELECT map('a', toUInt8(1)) IN [CAST(NULL AS Nullable(UUID))],
       map('a', toUInt8(1)) NOT IN [CAST(NULL AS Nullable(UUID))];
SELECT map('a', toUInt8(1)) IN materialize([CAST(NULL AS Nullable(UUID))]),
       map('a', toUInt8(1)) NOT IN materialize([CAST(NULL AS Nullable(UUID))]);
SELECT [toUInt8(1)] IN materialize([CAST(NULL AS Nullable(DateTime))]);
SELECT [toUInt8(1)] IN materialize([toUUID('00000000-0000-0000-0000-000000000001'), CAST(NULL AS Nullable(UUID))]);
SELECT [toUInt8(1)] IN materialize([CAST(toUInt8(1) AS Nullable(UInt8)), CAST(NULL AS Nullable(UInt8))]);
