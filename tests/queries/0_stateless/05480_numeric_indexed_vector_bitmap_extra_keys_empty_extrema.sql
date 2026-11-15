-- Indices present only in the `Bitmap` have value 1.
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 2], [toInt32(7), -3])) AS vec,
    bitmapBuild([toUInt32(2), 3]) AS bm
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(vec, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(vec, bm));

-- Empty `Int32`, `UInt64`, and `Float64` states return zero extrema.
SELECT numericIndexedVectorGetMaxValue(vec), numericIndexedVectorGetMinValue(vec)
FROM
(
    SELECT groupNumericIndexedVectorState(toUInt32(number), toInt32(number)) AS vec
    FROM numbers(0)
);

SELECT numericIndexedVectorGetMaxValue(vec), numericIndexedVectorGetMinValue(vec)
FROM
(
    SELECT groupNumericIndexedVectorState(toUInt32(number), toUInt64(number)) AS vec
    FROM numbers(0)
);

SELECT numericIndexedVectorGetMaxValue(vec), numericIndexedVectorGetMinValue(vec)
FROM
(
    SELECT groupNumericIndexedVectorState(toUInt32(number), toFloat64(number)) AS vec
    FROM numbers(0)
);
