SET in_null_as_unknown = 1;

SELECT
    [toUInt8(1)] IN CAST([toUInt8(1)] AS Array(Dynamic)),
    [toUInt8(1)] IN materialize(CAST([toUInt8(1)] AS Array(Dynamic)));

SELECT count() FROM values('a Array(UInt8)', ([1]))
WHERE a IN materialize(CAST([toUInt8(1)] AS Array(Dynamic)));

DROP TABLE IF EXISTS squality_equal_depth_array_carrier_check;
CREATE TABLE squality_equal_depth_array_carrier_check
(
    a Array(UInt8),
    b Array(Dynamic),
    CONSTRAINT c CHECK a IN b
) ENGINE = Memory;
INSERT INTO squality_equal_depth_array_carrier_check
SELECT [toUInt8(1)], CAST([toUInt8(1)] AS Array(Dynamic));
SELECT count() FROM squality_equal_depth_array_carrier_check WHERE a IN b;
DROP TABLE squality_equal_depth_array_carrier_check;
