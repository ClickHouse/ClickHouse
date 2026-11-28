SELECT
    toUInt8(0) IN (toDate(256), NULL),
    toUInt8(0) IN (materialize(toDate(256)), NULL),
    toUInt8(0) IN materialize([toDate(256), NULL])
SETTINGS in_null_as_unknown = 1;

SELECT
    toUInt8(0) IN (materialize(toDateTime(256)), NULL),
    toUInt8(0) IN materialize([toDateTime(256), NULL])
SETTINGS in_null_as_unknown = 1;

SELECT
    toDate('2000-01-01') IN (materialize(CAST(toDateTime('2000-01-01 12:00:00') AS Dynamic)), NULL),
    toDate('2000-01-01') IN materialize(CAST(toDateTime('2000-01-01 12:00:00') AS Dynamic))
SETTINGS in_null_as_unknown = 1;
