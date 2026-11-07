-- `quantileDD` must keep its relative accuracy for values with absolute value below 1

SELECT round(quantileDD(0.01)(0.5), 6), round(quantileDD(0.01)(0.99), 6), round(quantileDD(0.01)(-0.5), 6);

-- A sketch per value, both signs, both sides of 1
SELECT max(abs(q - x) / abs(x)) <= 0.01 * (1 + 1e-9)
FROM
(
    SELECT x, quantileDD(0.01)(x) AS q
    FROM (SELECT (number % 2 = 0 ? 1 : -1) * exp(-300 + number * 0.05) AS x FROM numbers(12000))
    GROUP BY x
);

-- Other accuracies
SELECT 0.001, max(abs(q - x) / abs(x)) <= 0.001 * (1 + 1e-9)
FROM (SELECT x, quantileDD(0.001)(x) AS q FROM (SELECT exp(-20 + number * 0.013) AS x FROM numbers(3000)) GROUP BY x);
SELECT 0.05, max(abs(q - x) / abs(x)) <= 0.05 * (1 + 1e-9)
FROM (SELECT x, quantileDD(0.05)(x) AS q FROM (SELECT exp(-20 + number * 0.013) AS x FROM numbers(3000)) GROUP BY x);
SELECT 0.3, max(abs(q - x) / abs(x)) <= 0.3 * (1 + 1e-9)
FROM (SELECT x, quantileDD(0.3)(x) AS q FROM (SELECT exp(-20 + number * 0.013) AS x FROM numbers(3000)) GROUP BY x);

-- Merging different accuracies must keep the counts of values below 1, else the median is about 0.9
SELECT round(quantilesDDMerge(0.01, 0.5)(s)[1], 6) FROM (
    SELECT quantilesDDState(0.01, 0.5)(x) AS s FROM (SELECT 0.1 + number / 100000 AS x FROM numbers(100))
    UNION ALL
    SELECT unhex(hex(quantilesDDState(0.05, 0.5)(0.9)))::AggregateFunction(quantilesDD(0.01, 0.5), Float64));

-- Gamma 4 is gamma 2 squared, so bin boundaries coincide: a value must not move to a neighbouring bin
-- holding a rounding sliver of its count
SELECT max(abs(q - x) / abs(x)) <= 0.6 FROM (
    SELECT x, quantilesDDMerge(0.3333333333333333, 0.5)(s)[1] AS q FROM (
        SELECT x, quantilesDDState(0.3333333333333333, 0.5)(x) AS s
        FROM (SELECT arrayJoin([1, -1]) * sqrt(2) * pow(2, number - 30) AS x FROM numbers(61)) GROUP BY x
        UNION ALL
        SELECT x, unhex(hex(quantilesDDStateIf(0.6, 0.5)(toFloat64(x), 0)))::AggregateFunction(quantilesDD(0.3333333333333333, 0.5), Float64)
        FROM (SELECT arrayJoin([1, -1]) * sqrt(2) * pow(2, number - 30) AS x FROM numbers(61)) GROUP BY x)
    GROUP BY x);

-- The partial lowest bin goes one bin up, so the state keeps keys that released versions accept when reading it
SELECT abs(quantileDDMerge(0.01)(s) - 2.28e-308) / 2.28e-308 <= 0.03 FROM (
    SELECT unhex(hex(quantileDDState(0.01)(2.28e-308)))::AggregateFunction(quantileDD(0.01), Float64) AS s);

-- The lowest and highest bins of a finer sketch start outside the range of a coarser mapping, and merging keeps their counts
SELECT q[1] > 0, q[1] < 3e-308, abs(q[2] - 1) <= 0.05 * (1 + 1e-9) FROM (
    SELECT quantilesDDMerge(0.01, 0, 0.5)(s) AS q FROM (
        SELECT quantilesDDState(0.01, 0, 0.5)(x) AS s FROM (SELECT arrayJoin([2.28e-308, 1, 1]) AS x)
        UNION ALL
        SELECT unhex(hex(quantilesDDState(0.05, 0, 0.5)(1)))::AggregateFunction(quantilesDD(0.01, 0, 0.5), Float64)));
SELECT abs(q[1] - 1) <= 0.05 * (1 + 1e-9), q[2] > 1.5e308, isFinite(q[2]) FROM (
    SELECT quantilesDDMerge(0.01, 0.5, 1)(s) AS q FROM (
        SELECT quantilesDDState(0.01, 0.5, 1)(x) AS s FROM (SELECT arrayJoin([1, 1, 1.7e308]) AS x)
        UNION ALL
        SELECT unhex(hex(quantilesDDState(0.05, 0.5, 1)(1)))::AggregateFunction(quantilesDD(0.01, 0.5, 1), Float64)));

-- The merged states of the two queries above must be readable: the highest new bin takes the counts above it
SELECT q[1] > 0, q[1] < 3e-308, abs(q[2] - 1) <= 0.05 * (1 + 1e-9) FROM (
    SELECT quantilesDDMerge(0.01, 0, 0.5)(unhex(h)::AggregateFunction(quantilesDD(0.01, 0, 0.5), Float64)) AS q FROM (
        SELECT hex(quantilesDDMergeState(0.01, 0, 0.5)(s)) AS h FROM (
            SELECT quantilesDDState(0.01, 0, 0.5)(x) AS s FROM (SELECT arrayJoin([2.28e-308, 1, 1]) AS x)
            UNION ALL
            SELECT unhex(hex(quantilesDDState(0.05, 0, 0.5)(1)))::AggregateFunction(quantilesDD(0.01, 0, 0.5), Float64))));
SELECT abs(q[1] - 1) <= 0.05 * (1 + 1e-9), q[2] > 1.5e308, isFinite(q[2]) FROM (
    SELECT quantilesDDMerge(0.01, 0.5, 1)(unhex(h)::AggregateFunction(quantilesDD(0.01, 0.5, 1), Float64)) AS q FROM (
        SELECT hex(quantilesDDMergeState(0.01, 0.5, 1)(s)) AS h FROM (
            SELECT quantilesDDState(0.01, 0.5, 1)(x) AS s FROM (SELECT arrayJoin([1, 1, 1.7e308]) AS x)
            UNION ALL
            SELECT unhex(hex(quantilesDDState(0.05, 0.5, 1)(1)))::AggregateFunction(quantilesDD(0.01, 0.5, 1), Float64))));

-- With this accuracy the upper bound of the highest bin overflows to inf. Remapping it onto a coarser mapping must keep
-- its count, else the merged sketch is empty and the result is nan
SELECT isFinite(q), q > 1e305 FROM (
    SELECT quantileDDMerge(0.99)(s) AS q FROM (
        SELECT unhex(hex(quantileDDState(0.9704489586316889)(1.7976931348623157e308 / 66.679510006868625)))::AggregateFunction(quantileDD(0.99), Float64) AS s));
