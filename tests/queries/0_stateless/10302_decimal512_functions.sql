-- Decimal512 conversion helpers
SELECT toDecimal512('42.000', 3);
SELECT toTypeName(toDecimal512('42', 3));
SELECT toDecimal512OrNull('bad', 3) IS NULL;
SELECT toDecimal512OrZero('bad', 3);
SELECT toDecimal512OrDefault('bad', 3, toDecimal512('1.234', 3));

-- arraySort with Decimal512 elements
SELECT arrayMap(x -> toString(x), arraySort([toDecimal512('3.10', 2), toDecimal512('1.50', 2)]));
SELECT toTypeName(arraySort([toDecimal512('3.10', 2), toDecimal512('1.50', 2)]));

-- arrayLevenshtein variants
SELECT arrayLevenshteinDistance(
    [toDecimal512('1.0', 1), toDecimal512('2.0', 1)],
    [toDecimal512('1.0', 1), toDecimal512('3.0', 1)]);

WITH
    CAST([], 'Array(Decimal(154, 1))') AS from_vals,
    CAST([toDecimal512('1.0', 1)], 'Array(Decimal(154, 1))') AS to_vals,
    CAST([], 'Array(Float64)') AS from_weights,
    CAST([0.5], 'Array(Float64)') AS to_weights
SELECT arrayLevenshteinDistanceWeighted(from_vals, to_vals, from_weights, to_weights);

-- mapAdd/mapSubtract Decimal512 aggregation
SELECT mapAdd(
    map('k1', toDecimal512('1.0', 1)),
    map('k1', toDecimal512('2.5', 1), 'k2', toDecimal512('0.5', 1)));
SELECT mapSubtract(
    map('k1', toDecimal512('3.5', 1), 'k2', toDecimal512('1.0', 1)),
    map('k1', toDecimal512('1.5', 1)));
SELECT toTypeName(mapAdd(map('k1', toDecimal512('1.0', 1)), map('k1', toDecimal512('2.5', 1))));

-- groupArrayMoving aggregations on Decimal512
SELECT arrayMap(x -> toString(x), groupArrayMovingSum(2)(toDecimal512(number, 1))) FROM numbers(3);
SELECT arrayMap(x -> toString(x), groupArrayMovingAvg(2)(toDecimal512(number + 2, 1))) FROM numbers(3);
SELECT toTypeName(groupArrayMovingSum(2)(toDecimal512(number, 1))) FROM numbers(1);
