SELECT
    toFloat64('nan') IN (toFloat64('nan'), NULL),
    toFloat64('nan') IN (materialize(CAST(toFloat64('nan') AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    toFloat64(1) IN (materialize(CAST(toDecimal32('1', 0) AS Dynamic)), NULL),
    toUInt8(1) IN (materialize(CAST(toDecimal32('1', 0) AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    toFloat64(1) IN (SELECT toDecimal32('1', 0)),
    toUInt8(1) IN (SELECT toDecimal32('1', 0))
SETTINGS in_null_as_unknown = 1;

SELECT
    '01' IN CAST(toUInt8(1) AS Dynamic),
    '01' IN materialize(CAST(toUInt8(1) AS Dynamic))
SETTINGS in_null_as_unknown = 1;

SELECT
    [tuple(toFloat64(0.1))] IN (materialize([tuple(toDecimal32('0.1', 1))]), NULL),
    [tuple(toFloat64(0.1))] IN (materialize(CAST([tuple(toDecimal32('0.1', 1))] AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT [CAST(NULL AS Nullable(Float64)), toFloat64(0.1)] IN
    (materialize(CAST([CAST(NULL AS Nullable(Decimal32(1))), toDecimal32('0.1', 1)] AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_in_string_carrier_check;
CREATE TABLE squality_in_string_carrier_check
(
    s String,
    v Dynamic,
    CONSTRAINT c CHECK s IN (v, NULL)
) ENGINE = Memory;
INSERT INTO squality_in_string_carrier_check SELECT '1', CAST(toUInt8(1) AS Dynamic)
SETTINGS in_null_as_unknown = 1;
SELECT count() FROM squality_in_string_carrier_check;
DROP TABLE squality_in_string_carrier_check;
