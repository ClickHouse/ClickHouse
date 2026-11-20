SET in_null_as_unknown = 1;

SELECT [toDecimal128('9007199254740993', 0)] IN
    materialize([toFloat64(9007199254740992)]);
SELECT [toDecimal128('9007199254740993', 0)] IN
    (SELECT [toFloat64(9007199254740992)]); -- { serverError NOT_IMPLEMENTED }

SELECT map('a', toDecimal128('9007199254740993', 0)) IN
    (SELECT map('a', toFloat64(9007199254740992))); -- { serverError NOT_IMPLEMENTED }
