-- Division by scalar, vector, and `Bitmap` ones must preserve values above 2^53.
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 2, 3], [toUInt64(9007199254740993), 9223372036854775807, 0])) AS v,
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 2, 3], [toUInt64(1), 1, 1])) AS ones
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, toUInt64(1))),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, ones)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, bitmapBuild([toUInt32(1), 2, 3])));

WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 2, 3, 4], [toInt64(9007199254740993), -9007199254740993, 9223372036854775807, -9223372036854775808])) AS v,
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 2, 3, 4], [toInt64(1), 1, 1, 1])) AS ones
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, toInt64(1))),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, ones)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, bitmapBuild([toUInt32(1), 2, 3, 4])));

-- Keep exact values on shared indexes and explicit zeros on all other indexes.
WITH
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 2], [toUInt64(9007199254740993), 17])) AS v,
    numericIndexedVectorBuild(mapFromArrays([toUInt32(1), 3], [toUInt64(1), 1])) AS ones
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, ones)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, bitmapBuild([toUInt32(1), 3])));

-- A single signed integer bit encodes -1, so it must not take a positive-one shortcut.
WITH
    (SELECT groupNumericIndexedVectorState('BSI', 1, 0)(toUInt32(1), toInt32(-1))) AS minus_one,
    numericIndexedVectorBuild(map(toUInt32(1), toInt32(7))) AS v
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, minus_one)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, minus_one)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(minus_one, v)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(minus_one, toInt32(2)));
