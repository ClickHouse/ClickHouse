SET in_null_as_unknown = 1;

SELECT
    isNull(map('01', toFloat64(1)) IN
        (materialize(map(toUInt8(1), toDecimal32(1, 0))), NULL)),
    isNull(map('01', toFloat64(1)) IN
        (materialize(CAST(map(toUInt8(1), toDecimal32(1, 0)) AS Dynamic)), NULL));

SELECT count() FROM (SELECT map('01', toFloat64(1)) AS m)
WHERE m IN (materialize(map(toUInt8(1), toDecimal32(1, 0))), NULL);

SELECT count() FROM (SELECT map('01', toFloat64(1)) AS m)
WHERE m IN (materialize(CAST(map(toUInt8(1), toDecimal32(1, 0)) AS Dynamic)), NULL);

SELECT map('1', toFloat64(1)) IN
    (materialize(CAST(map(toUInt8(1), toDecimal32(1, 0)) AS Dynamic)), NULL);
