SET in_null_as_unknown = 1;

SELECT toDecimal128('9007199254740993', 0)
    IN (SELECT toFloat64(9007199254740992));

SELECT CAST(toDecimal128('9007199254740993', 0) AS Variant(Decimal(38, 0), String))
    IN (SELECT toFloat64(9007199254740992)); -- { serverError NOT_IMPLEMENTED }

SELECT count()
FROM (SELECT CAST(toDecimal128('9007199254740993', 0) AS Variant(Decimal(38, 0), String)) AS v)
WHERE v IN (SELECT toFloat64(9007199254740992)); -- { serverError NOT_IMPLEMENTED }

SELECT CAST([toDecimal128('9007199254740993', 0)] AS Array(Variant(Decimal(38, 0), String)))
    IN (SELECT [toFloat64(9007199254740992)]); -- { serverError NOT_IMPLEMENTED }

SELECT count()
FROM (SELECT CAST([toDecimal128('9007199254740993', 0)] AS Array(Variant(Decimal(38, 0), String))) AS v)
WHERE v IN (SELECT [toFloat64(9007199254740992)]); -- { serverError NOT_IMPLEMENTED }
