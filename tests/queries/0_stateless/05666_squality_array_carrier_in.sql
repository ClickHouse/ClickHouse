SET in_null_as_unknown = 1;

SELECT
    [toUInt8(1)] IN (materialize([toUInt16(1)]), NULL),
    [toUInt8(1)] IN (materialize(CAST([toUInt16(1)] AS Dynamic)), NULL),
    [toUInt8(1)] IN (materialize(CAST([toUInt16(1)] AS Variant(Array(UInt16), String))), NULL);

SELECT
    [toUInt8(2)] IN (materialize(CAST([toUInt16(1)] AS Dynamic)), NULL),
    [toUInt8(2)] IN (materialize(CAST([toUInt16(1)] AS Variant(Array(UInt16), String))), NULL),
    [toUInt8(1)] IN (materialize(CAST('other' AS Variant(Array(UInt16), String))), NULL);

SELECT
    [toDecimal32('0.1', 1)] IN (materialize(CAST([toFloat64(0.1)] AS Dynamic)), NULL),
    [toFloat64(0.1)] IN (materialize(CAST([toDecimal32('0.1', 1)] AS Variant(Array(Decimal32(1)), String))), NULL),
    [toDecimal128('9007199254740993', 0)] IN
        (materialize(CAST([toFloat64(9007199254740992)] AS Dynamic)), NULL);

SELECT count()
FROM (SELECT [toUInt8(number)] AS a FROM numbers(2))
WHERE a IN (materialize(CAST([toUInt16(1)] AS Dynamic)), NULL);

DROP TABLE IF EXISTS squality_array_carrier_in;
CREATE TABLE squality_array_carrier_in
(
    x Array(UInt8),
    d Dynamic,
    v Variant(Array(UInt16), String),
    CONSTRAINT dynamic_match CHECK x IN (d, NULL),
    CONSTRAINT variant_match CHECK x IN (v, NULL)
) ENGINE = Memory;
INSERT INTO squality_array_carrier_in
SELECT [toUInt8(1)], CAST([toUInt16(1)] AS Dynamic),
       CAST([toUInt16(1)] AS Variant(Array(UInt16), String));
SELECT x IN (d, NULL), x IN (v, NULL) FROM squality_array_carrier_in;
DROP TABLE squality_array_carrier_in;
