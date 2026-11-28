SET in_null_as_unknown = 1;

SELECT
    [toUInt8(1)] IN CAST([toUInt8(1)] AS Array(Dynamic)),
    [toUInt8(1)] IN materialize(CAST([toUInt8(1)] AS Array(Dynamic)));

SELECT [toUInt8(1)] IN materialize([CAST([toUInt8(1)] AS Dynamic)]);

SELECT
    emptyArrayUInt8() IN CAST([] AS Array(Dynamic)),
    emptyArrayUInt8() IN materialize(CAST([] AS Array(Dynamic))),
    [toUInt8(1)] IN CAST([NULL] AS Array(Dynamic)),
    [toUInt8(1)] IN materialize(CAST([NULL] AS Array(Dynamic)));

SELECT count() FROM values('a Array(UInt8)', ([]))
WHERE a IN materialize(CAST([] AS Array(Dynamic)));

SELECT count() FROM values('a Array(UInt8)', ([1]))
WHERE a IN materialize(CAST([toUInt8(1)] AS Array(Dynamic)));

DROP TABLE IF EXISTS squality_equal_depth_array_carrier_check;
CREATE TABLE squality_equal_depth_array_carrier_check
(
    a Array(UInt8),
    b Array(Dynamic),
    CONSTRAINT c CHECK a IN materialize(b)
) ENGINE = Memory;
INSERT INTO squality_equal_depth_array_carrier_check
SELECT [toUInt8(1)], CAST([toUInt8(1)] AS Array(Dynamic));
SELECT count() FROM squality_equal_depth_array_carrier_check WHERE a IN materialize(b);
DROP TABLE squality_equal_depth_array_carrier_check;
