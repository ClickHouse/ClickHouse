SELECT toUInt8(1) IN materialize(CAST(toUInt16(1) AS Dynamic))
SETTINGS in_null_as_unknown = 1;

SELECT toUInt8(1) IN (SELECT CAST(toUInt16(1) AS Dynamic))
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }

SELECT toUInt8(1) IN (SELECT CAST(toUInt16(1) AS Variant(UInt16, String)))
SETTINGS in_null_as_unknown = 1; -- { serverError NOT_IMPLEMENTED }
