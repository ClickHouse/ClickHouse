SET in_null_as_unknown = 1;

SELECT toDecimal128('9007199254740993', 0) IN [toFloat64(9007199254740992), NULL],
       toDecimal128('9007199254740993', 0) IN materialize([toFloat64(9007199254740992), NULL]);
SELECT toDecimal128('9007199254740993', 0) NOT IN [toFloat64(9007199254740992), NULL],
       toDecimal128('9007199254740993', 0) NOT IN materialize([toFloat64(9007199254740992), NULL]);
SELECT toDecimal32('0.1', 1) IN [toFloat64(0.1), NULL],
       toDecimal32('0.1', 1) IN materialize([toFloat64(0.1), NULL]);

SELECT d IN a, d NOT IN a
FROM VALUES('d Decimal(38, 0), a Array(Nullable(Float64))',
    ('9007199254740993', [9007199254740992, NULL]));
SELECT count() FROM VALUES('d Decimal(38, 0), a Array(Nullable(Float64))',
    ('9007199254740993', [9007199254740992, NULL]))
WHERE d IN a;
