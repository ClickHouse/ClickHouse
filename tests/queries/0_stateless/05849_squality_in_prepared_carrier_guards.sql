SET in_null_as_unknown = 1;

SELECT '1' IN materialize(CAST(toUInt8(1) AS Dynamic));
SELECT '1' IN (SELECT CAST(toUInt8(1) AS Dynamic)); -- { serverError NOT_IMPLEMENTED }
SELECT '1' IN (SELECT CAST(toUInt8(1) AS Variant(UInt8, String))); -- { serverError NOT_IMPLEMENTED }

SELECT [toUInt8(1)] IN (SELECT CAST([toUInt8(1)] AS Dynamic)); -- { serverError NOT_IMPLEMENTED }
SELECT map('a', toUInt8(1)) IN (SELECT CAST(map('a', toUInt8(1)) AS Dynamic)); -- { serverError NOT_IMPLEMENTED }
