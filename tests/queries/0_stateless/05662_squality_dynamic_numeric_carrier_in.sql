SELECT
    toUInt8(1) IN (CAST(toUInt16(1) AS Dynamic), NULL),
    toUInt8(1) IN (materialize(CAST(toUInt16(1) AS Dynamic)), NULL),
    toUInt8(1) IN materialize(CAST(toUInt16(1) AS Dynamic)),
    toUInt8(1) NOT IN (materialize(CAST(toUInt16(1) AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT
    toUInt8(2) IN (materialize(CAST(toUInt16(1) AS Dynamic)), NULL),
    toUInt8(2) NOT IN (materialize(CAST(toUInt16(1) AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count()
FROM (SELECT toUInt8(number) AS x FROM numbers(2))
WHERE x IN (materialize(CAST(toUInt16(1) AS Dynamic)), NULL)
SETTINGS in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_dynamic_numeric_carrier_in;
SET in_null_as_unknown = 1;
CREATE TABLE squality_dynamic_numeric_carrier_in
(
    x UInt8,
    v Dynamic,
    CONSTRAINT matched CHECK x IN (v, NULL)
)
ENGINE = Memory;
INSERT INTO squality_dynamic_numeric_carrier_in SELECT toUInt8(1), CAST(toUInt16(1) AS Dynamic);
SELECT count() FROM squality_dynamic_numeric_carrier_in;
DROP TABLE squality_dynamic_numeric_carrier_in;
