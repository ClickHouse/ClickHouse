WITH
    (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(1), toInt32(0))) AS lhs,
    (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(2), toInt32(0))) AS rhs,
    (SELECT groupNumericIndexedVectorStateIf('BSI', 0, 0)(toUInt32(3), toInt32(0), false)) AS empty
SELECT numericIndexedVectorToMap(v), numericIndexedVectorCardinality(v)
FROM
(
    SELECT arrayJoin([
        lhs,
        rhs,
        numericIndexedVectorPointwiseMax(lhs, lhs),
        numericIndexedVectorPointwiseMin(lhs, lhs),
        numericIndexedVectorPointwiseMax(lhs, rhs),
        numericIndexedVectorPointwiseMin(lhs, rhs),
        numericIndexedVectorPointwiseMax(lhs, empty),
        numericIndexedVectorPointwiseMin(lhs, empty),
        numericIndexedVectorPointwiseMax(empty, rhs),
        numericIndexedVectorPointwiseMin(empty, rhs),
        numericIndexedVectorPointwiseMax(empty, empty),
        numericIndexedVectorPointwiseMin(empty, empty)
    ]) AS v
);

WITH
    (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(1), toUInt64(0))) AS lhs,
    (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(2), toUInt64(0))) AS rhs
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(lhs, rhs)),
    numericIndexedVectorCardinality(numericIndexedVectorPointwiseMax(lhs, rhs)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(lhs, rhs)),
    numericIndexedVectorCardinality(numericIndexedVectorPointwiseMin(lhs, rhs));

WITH (SELECT groupNumericIndexedVectorState('BSI', 0, 0)(toUInt32(1), toFloat64(0))) AS v
SELECT
    numericIndexedVectorToMap(v),
    numericIndexedVectorCardinality(v),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMax(v, v)),
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMin(v, v));
