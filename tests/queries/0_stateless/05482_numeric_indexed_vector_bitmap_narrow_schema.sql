-- A `Bitmap` contributes positive ones even when the source has no integer bits.
WITH
    (SELECT groupNumericIndexedVectorState('BSI', 0, 8)(toUInt32(1), toFloat64(0.25))) AS v,
    bitmapBuild([toUInt32(1), 2]) AS bm
SELECT arrayJoin([
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm))
]);

-- With one signed integer bit, its only integer slice is the sign bit.
WITH
    (SELECT groupNumericIndexedVectorState('BSI', 1, 8)(toUInt32(1), toFloat64(0.25))) AS v,
    bitmapBuild([toUInt32(1), 2]) AS bm
SELECT arrayJoin([
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm))
]);

WITH
    (SELECT groupNumericIndexedVectorState('BSI', 1, 8)(toUInt32(1), toFloat32(0.25))) AS v,
    bitmapBuild([toUInt32(1), 2]) AS bm
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm));

-- Zero-bit signed and unsigned states still carry explicit zero indexes.
WITH
    (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(1), toInt32(0))) AS v,
    bitmapBuild([toUInt32(1), 2]) AS bm
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm));

WITH
    (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(1), toUInt32(0))) AS v,
    bitmapBuild([toUInt32(1), 2]) AS bm
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, bm));

WITH
    (SELECT groupNumericIndexedVectorState('BSI', 1, 0)(toUInt32(1), toUInt32(1))) AS v,
    bitmapBuild([toUInt32(1), 2]) AS bm
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm));

-- Empty bitmaps contribute no indexes after the temporary schema is widened.
WITH
    (SELECT groupNumericIndexedVectorState('BSI', 0, 8)(toUInt32(1), toFloat64(0.25))) AS v,
    bitmapBuild(CAST([], 'Array(UInt32)')) AS bm
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(v, bm)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(v, bm));
