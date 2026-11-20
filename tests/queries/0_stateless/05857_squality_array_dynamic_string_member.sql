SET in_null_as_unknown = 1;

SELECT
    [toUInt8(1)] IN CAST(['1'] AS Array(Dynamic)),
    [toUInt8(1)] IN materialize(CAST(['1'] AS Array(Dynamic)));

SELECT count() FROM values('a Array(UInt8)', ([1]))
WHERE a IN materialize(CAST(['1'] AS Array(Dynamic)));

DROP TABLE IF EXISTS squality_array_dynamic_string_check;
CREATE TABLE squality_array_dynamic_string_check
(
    a Array(UInt8),
    b Array(Dynamic),
    CONSTRAINT c CHECK a IN materialize(b)
) ENGINE = Memory;
INSERT INTO squality_array_dynamic_string_check
SELECT [toUInt8(1)], CAST(['1'] AS Array(Dynamic));
SELECT count() FROM squality_array_dynamic_string_check WHERE a IN materialize(b);
DROP TABLE squality_array_dynamic_string_check;
