SET in_null_as_unknown = 1;

SELECT toUInt8(1) IN (SELECT toUInt8(1));
SELECT toUInt8(1) IN (SELECT toUInt16(1));
SELECT toDate('2000-01-01') IN (SELECT toDate('2000-01-01'));

SELECT toDecimal128('9007199254740993', 0) IN
    (SELECT arrayJoin([toFloat64(9007199254740992), NULL]));
SELECT toDecimal128('9007199254740993', 0) IN
    (SELECT toFloat64(9007199254740992));
SELECT count()
FROM (SELECT toDecimal128('9007199254740993', 0) AS d)
WHERE d IN (SELECT arrayJoin([toFloat64(9007199254740992), NULL]));
SELECT toDate('2000-01-01') IN
    (SELECT toDateTime('2000-01-01 12:00:00')); -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS squality_prepared_float_set;
CREATE TABLE squality_prepared_float_set (k Nullable(Float64)) ENGINE = Set;
INSERT INTO squality_prepared_float_set VALUES (9007199254740992), (NULL);
SELECT toDecimal128('9007199254740993', 0) IN squality_prepared_float_set;
DROP TABLE squality_prepared_float_set;
