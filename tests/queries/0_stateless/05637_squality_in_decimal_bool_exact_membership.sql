SET in_null_as_unknown = 1;

SELECT toDecimal128('9007199254740993', 0) IN (toFloat64(9007199254740992), NULL),
       toDecimal128('9007199254740993', 0) IN (materialize(toFloat64(9007199254740992)), NULL);
SELECT toDecimal128('9007199254740993', 0) NOT IN (toFloat64(9007199254740992), NULL),
       toDecimal128('9007199254740993', 0) NOT IN (materialize(toFloat64(9007199254740992)), NULL);
SELECT count() FROM VALUES('x Decimal128(0)', ('9007199254740993'))
WHERE x IN (materialize(toFloat64(9007199254740992)), NULL);

SELECT toDecimal128('0.1', 1) IN (toFloat64(0.1), NULL),
       toDecimal128('0.1', 1) IN (materialize(toFloat64(0.1)), NULL);
SELECT toDecimal128('1', 0) IN (materialize(toFloat64(1.5)), NULL);
SELECT toDecimal64('1', 0) IN (toBFloat16(1), NULL),
       toDecimal64('1', 0) IN (materialize(toBFloat16(1)), NULL);

SELECT true IN (2, NULL), true IN (materialize(2), NULL);
SELECT true NOT IN (2, NULL), true NOT IN (materialize(2), NULL);
SELECT true IN (materialize(1), NULL);
SELECT count() FROM VALUES('x Bool', (true)) WHERE x IN (materialize(2), NULL);
