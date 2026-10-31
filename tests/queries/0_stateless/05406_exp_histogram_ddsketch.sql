-- `fromExpHistogram` and `toExpHistogram`: conversion between OpenTelemetry exponential histograms and `quantileDD` states

SELECT 'round trip';
SELECT scale, toExpHistogram(fromExpHistogram(scale, 7, -3, [1, 0, 2, 3], 5, [4, 5])) AS h, h.scale = scale
FROM (SELECT arrayJoin([-5, -2, 0, 3, 8, 18]) AS scale);
SELECT toExpHistogram(fromExpHistogram(0, 0, 0, [], 0, []));
SELECT toExpHistogram(fromExpHistogram(2, 3, 0, [], 0, []));
SELECT toExpHistogram(fromExpHistogram(toInt8(1), toUInt8(1), toUInt16(2), [toUInt32(3)], toInt32(-4), [toUInt8(5)]));
-- Zeros at the edges of the bucket arrays are trimmed
SELECT toExpHistogram(fromExpHistogram(1, 0, 10, [0, 0, 1, 0, 2, 0], 0, [0]));

SELECT 'scales finer than 18';
-- Downscaled to scale 18: bucket `i` of scale `s` goes to bucket `i >> (s - 18)`
SELECT toExpHistogram(fromExpHistogram(19, 0, 5, [1, 2, 3], -3, [4, 5]));
SELECT toExpHistogram(fromExpHistogram(20, 1, -5, [1, 2, 3, 4, 5], 0, []));
-- Data points of scales 18 and 20 merge exactly
SELECT toExpHistogram(quantileDDMergeState(0.000001)(state))
FROM
(
    SELECT fromExpHistogram(18, 0, 1000000, [1, 2, 3], 0, []) AS state
    UNION ALL
    SELECT fromExpHistogram(20, 0, 4000004, [1, 1, 1, 1, 2, 2, 2, 2], 0, [])
);

SELECT 'far buckets';
-- The base of scale 18 is not exact in floating point, but bins stay buckets however far they are from 1
SELECT toExpHistogram(fromExpHistogram(18, 0, 100000000, [1, 2], 0, [3]));
SELECT toExpHistogram(quantileDDMergeState(0.000001)(state))
FROM
(
    SELECT fromExpHistogram(18, 0, -100000000, [1, 2], 0, []) AS state
    UNION ALL
    SELECT fromExpHistogram(18, 0, -100000001, [4], 0, [])
);
SELECT toExpHistogram(quantileDDMergeState(0.01)(fromExpHistogram(-5, 0, -30, [1, 2], 30, [3])));
-- The bins of a state cover a bit less than the finite numbers: the buckets of the smallest of them (subnormal numbers are
-- in buckets -1075 to -1023 of scale 0) and of the largest of them go into the edge bins. The exact edges depend on rounding
SELECT h.count, h.positive_offset BETWEEN -1022 AND -1020, h.positive_bucket_counts, h.negative_offset, h.negative_bucket_counts
FROM (SELECT toExpHistogram(fromExpHistogram(0, 0, -1075, [1, 2, 3], 1000, [4])) AS h);
-- 1073741820 = 1024 · 2²⁰ − 4: the 4 highest buckets of scale 20, up to 2¹⁰²⁴
SELECT h.count, h.scale, h.positive_offset BETWEEN 268435453 AND 268435455, h.positive_bucket_counts
FROM (SELECT toExpHistogram(fromExpHistogram(20, 0, 1073741820, [1, 2, 3, 4], 0, [])) AS h);

SELECT 'resolution of merges';
-- Relative accuracy 0.01 keeps the scales up to 5, 0.0001 up to 11, 0.000001 all of them
SELECT
    scale,
    toExpHistogram(quantileDDMergeState(0.01)(state)).scale,
    toExpHistogram(quantileDDMergeState(0.0001)(state)).scale,
    toExpHistogram(quantileDDMergeState(0.000001)(state)).scale
FROM (SELECT scale, fromExpHistogram(scale, 0, 0, [1], 0, []) AS state FROM (SELECT arrayJoin([5, 6, 11, 12, 18]) AS scale))
GROUP BY scale
ORDER BY scale;
-- Data points without buckets (only zeros, or empty) do not lower the resolution, in either order
SELECT
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [fine, empty])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [empty, fine])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [fine, zeros])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [zeros, fine])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [zeros, empty]))
FROM
(
    SELECT
        fromExpHistogram(18, 0, 1000, [1, 2, 3], 0, []) AS fine,
        fromExpHistogram(0, 0, 0, [], 0, []) AS empty,
        fromExpHistogram(-5, 7, 0, [], 0, []) AS zeros
);
-- A state of a scale and a state of values: the result does not depend on the order of the merge
SELECT
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [s, v])).scale,
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [s, v])) = toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [v, s])),
    arrayReduce('quantileDDMerge(0.000001, 0.5)', [s, v]) = arrayReduce('quantileDDMerge(0.000001, 0.5)', [v, s])
FROM
(
    SELECT
        fromExpHistogram(18, 0, 1000, [1, 2, 3], 0, []) AS s,
        (SELECT CAST(unhex(hex(quantileDDState(0.01)(toFloat64(number + 1)))), 'AggregateFunction(quantileDD(0.000001), Float64)') FROM numbers(10)) AS v
);
-- Background merges of `AggregatingMergeTree` have the resolution of the parameter of the type. The first part has only
-- zeros at scale -5
DROP TABLE IF EXISTS exp_histograms;
CREATE TABLE exp_histograms (k UInt8, coarse AggregateFunction(quantileDD(0.01), Float64), fine AggregateFunction(quantileDD(0.000001), Float64))
ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO exp_histograms SELECT 0, fromExpHistogram(-5, 2, 0, [], 0, [], 0.01), fromExpHistogram(-5, 2, 0, [], 0, []);
INSERT INTO exp_histograms SELECT 0, fromExpHistogram(8, 0, 100, [1, 2, 3], 0, [], 0.01), fromExpHistogram(8, 0, 100, [1, 2, 3], 0, []);
INSERT INTO exp_histograms SELECT 0, fromExpHistogram(8, 0, 103, [4], 0, [], 0.01), fromExpHistogram(18, 0, 103 * 1024, [4], 0, []);
OPTIMIZE TABLE exp_histograms FINAL;
SELECT toExpHistogram(coarse), toExpHistogram(fine) FROM exp_histograms;
DROP TABLE exp_histograms;
-- Copies of states are merges into new states of the type: with the default relative accuracy they keep the scale
SELECT toExpHistogram(if(number = 0, fromExpHistogram(18, 0, 1000, [1, 2, 3], 0, []), fromExpHistogram(8, 0, 1000, [1, 2, 3], 0, []))).scale
FROM numbers(2);
SELECT abs(finalizeAggregation(fromExpHistogram(18, 0, 100000, [1], 0, [])) / pow(2, (100000 + 0.5) / 262144) - 1) < 0.000001;
-- Relative accuracy 0.01 is between scales 5 and 6: scale 6 is downscaled exactly to 5, whatever the order of the merge.
-- `arrayJoin` keeps the order of the states
SELECT toExpHistogram(quantileDDMergeState(0.01)(s))
FROM (SELECT arrayJoin([fromExpHistogram(5, 0, 100, [1, 2, 3, 4], 0, []), fromExpHistogram(6, 0, 200, [5, 6, 7, 8, 9, 10, 11, 12], 0, [])]) AS s);
SELECT toExpHistogram(quantileDDMergeState(0.01)(s))
FROM (SELECT arrayJoin([fromExpHistogram(6, 0, 200, [5, 6, 7, 8, 9, 10, 11, 12], 0, []), fromExpHistogram(5, 0, 100, [1, 2, 3, 4], 0, [])]) AS s);

SELECT 'type';
SELECT toTypeName(fromExpHistogram(0, 0, 0, [1], 0, []));
SELECT toTypeName(fromExpHistogram(0, 0, 0, [1], 0, [], 0.001));

SELECT 'quantiles of bucketed data';
-- Bucket `i` of scale `s` holds the values in (2^(i / 2^s), 2^((i + 1) / 2^s)]
WITH buckets AS
(
    SELECT scale, lo AS offset, arrayMap(j -> toUInt64(countEqual(indexes, lo + j)), range(toUInt64(hi - lo + 1))) AS counts
    FROM
    (
        SELECT scale, groupArray(i) AS indexes, min(i) AS lo, max(i) AS hi
        FROM
        (
            SELECT scale, toInt64(ceil(log2(number + 1) * pow(2, scale))) - 1 AS i
            FROM numbers(1000) AS n
            CROSS JOIN (SELECT arrayJoin([0, 3, 6]) AS scale) AS scales
        )
        GROUP BY scale
    )
)
SELECT
    scale,
    arrayMap(
        (dd, exact) -> abs(dd - exact) / exact <= (pow(2, pow(2, -scale)) - 1) / (pow(2, pow(2, -scale)) + 1) + 0.01,
        quantilesDDMerge(0.0001, 0.1, 0.5, 0.9, 0.99)(fromExpHistogram(scale, 0, offset, counts, 0, [])),
        (SELECT quantilesExact(0.1, 0.5, 0.9, 0.99)(number + 1) FROM numbers(1000)))
FROM buckets
GROUP BY scale
ORDER BY scale;

SELECT 'negative values and zeros';
SELECT quantilesDDMerge(0.0001, 0, 0.25, 0.5, 0.75, 1)(fromExpHistogram(0, 2, 0, [2], 0, [2]));

SELECT 'merge of different scales';
-- Scale 3 is downscaled to scale 1 exactly: buckets 4 * i ... 4 * i + 3 of scale 3 make bucket i of scale 1
SELECT toExpHistogram(quantileDDMergeState(0.0001)(state))
FROM
(
    SELECT fromExpHistogram(3, 1, 0, [1, 2, 3, 4, 5], 2, [1, 1]) AS state
    UNION ALL
    SELECT fromExpHistogram(1, 2, -1, [10, 20], 0, [30])
);
SELECT toExpHistogram(fromExpHistogram(1, 3, -1, [10, 30, 5], 0, [32]));
SELECT toExpHistogram(fromExpHistogram(3, 1, 0, [1, 2, 3, 4, 5], 2, [1, 1]), 1);

SELECT 'NULLs';
SELECT toExpHistogram(fromExpHistogram(3, 0, 0, [1, 2, 3, 4, 5], 0, []), if(number = 0, NULL, 1)) FROM numbers(2);
SELECT fromExpHistogram(NULL, 0, 0, [1], 0, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(if(number = 0, NULL, 1), 0, 0, [1], 0, []) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(0, toNullable(0), 0, [1], 0, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(0, 0, 0, [1], toNullable(0), []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'values too far apart';
-- Scale 14 is finer than `relative_accuracy = 0.0001`, so the merge downscales it to scale 11, the finest that is not finer,
-- and 65536 bins of scale 11 cover a factor of `2^32`. The values are about `2^37` apart, so 2 bins merge into one: scale 10
SELECT abs(quantileDDMerge(0.0001, 0.5)(state) / pow(2, -300000 / 16384) - 1) < 0.0005,
    toExpHistogram(quantileDDMergeState(0.0001)(state)).scale
FROM
(
    SELECT fromExpHistogram(14, 0, -300000, [1], 0, []) AS state
    UNION ALL
    SELECT fromExpHistogram(14, 0, 300000, [1], 0, [])
);
SELECT abs(quantileDDMerge(0.001, 0.5)(state) / pow(2, -163000 / 16384) - 1) < 0.002
FROM
(
    SELECT fromExpHistogram(14, 0, -163000, [1], 0, []) AS state
    UNION ALL
    SELECT fromExpHistogram(14, 0, 163000, [1], 0, [])
);
-- 70000 buckets of scale 18 do not fit into 65536 bins, they fit at scale 17
SELECT (toExpHistogram(fromExpHistogram(18, 0, 0, arrayWithConstant(70000, 1), 0, [])) AS h).scale,
    h.positive_offset, length(h.positive_bucket_counts), arraySum(h.positive_bucket_counts);
-- 65536 bins of relative accuracy 0.000001 cover a factor of about 1.14. 1 and 500 are about 3.1e6 bins apart, so 64 bins
-- merge into one. Not a scale: reported as scale 12, the finest with buckets not narrower than the bins
SELECT arrayMap((q, v) -> abs(q / v - 1) < 0.0001, quantilesDD(0.000001, 0.1, 0.9)(x), [-400, 400]),
    toExpHistogram(quantileDDState(0.000001)(x)).scale
FROM (SELECT toFloat64(number) - 500 AS x FROM numbers(1001));
-- So do the merges of its states: 1 and 2 are about 346574 bins apart, so 8 bins merge into one
SELECT arrayMap((q, v) -> abs(q / v - 1) < 0.00001, quantilesDDMerge(0.000001, 0, 1)(state), [1, 2])
FROM (SELECT quantileDDState(0.000001)(toFloat64(number + 1)) AS state FROM numbers(2) GROUP BY number);
-- An exception would repeat in every background merge of `AggregatingMergeTree`. The states of scale 14 are coarser than
-- the type, so the merge keeps their bins. They are 163290 buckets apart, about 2.5 · 65536, so 4 of them merge into
-- one: scale 12
DROP TABLE IF EXISTS exp_histogram_far;
CREATE TABLE exp_histogram_far (k UInt8, state AggregateFunction(quantileDD(0.000001), Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO exp_histogram_far SELECT 0, fromExpHistogram(14, 0, -163290, [1], 0, []);
INSERT INTO exp_histogram_far SELECT 0, fromExpHistogram(14, 0, 0, [1], 0, []);
OPTIMIZE TABLE exp_histogram_far FINAL;
SELECT count(), (toExpHistogram(any(state)) AS h).count, h.scale, h.positive_offset, length(h.positive_bucket_counts),
    arraySum(h.positive_bucket_counts)
FROM exp_histogram_far;
DROP TABLE exp_histogram_far;
-- The values of each part fit into 65536 bins of relative accuracy 0.000001, but not together: about 320927 bins. The
-- resolution is the same whether the values go into one state or the states of the parts are merged
DROP TABLE IF EXISTS exp_histogram_parts;
CREATE TABLE exp_histogram_parts (x Float64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES exp_histogram_parts;
INSERT INTO exp_histogram_parts SELECT 1 + number / 1e6 FROM numbers(100);
INSERT INTO exp_histogram_parts SELECT 1.9 + number / 1e6 FROM numbers(100);
SELECT round(quantileDD(0.000001, 0.5)(x), 6), toExpHistogram(quantileDDState(0.000001)(x)).scale FROM exp_histogram_parts SETTINGS max_threads = 1;
SELECT round(quantileDD(0.000001, 0.5)(x), 6), toExpHistogram(quantileDDState(0.000001)(x)).scale FROM exp_histogram_parts SETTINGS max_threads = 4;
SELECT round(quantileDDMerge(0.000001, 0.5)(state), 6), toExpHistogram(quantileDDMergeState(0.000001)(state)).scale
FROM (SELECT _part, quantileDDState(0.000001)(x) AS state FROM exp_histogram_parts GROUP BY _part);
DROP TABLE exp_histogram_parts;
-- Downscaling by 1 and then by 2 must give the same mapping as by 3: e.g. [1, 1.2, x] and [1, x]. Otherwise the merge
-- remaps, which needs a bin more at each end, and the values take 65535 of the 65536 bins. x = 2.8535162336408666 is
-- γ^(8 · 65534.5) for relative accuracy 0.000001: the middle of bin 65534 once 8 bins merge into one
SELECT toExpHistogram(quantileDDState(0.000001)(v)).scale, arrayMap(q -> round(q, 6), quantilesDD(0.000001, 0.01, 0.99)(v))
FROM (SELECT arrayJoin([1, 1.2, 2.8535162336408666, 1, 2.8535162336408666]) AS v);
SELECT toExpHistogram(quantileDDMergeState(0.000001)(s)).scale, arrayMap(q -> round(q, 6), quantilesDDMerge(0.000001, 0.01, 0.99)(s))
FROM (SELECT arrayJoin([
    (SELECT quantileDDState(0.000001)(v) FROM (SELECT arrayJoin([1, 1.2, 2.8535162336408666]) AS v)),
    (SELECT quantileDDState(0.000001)(v) FROM (SELECT arrayJoin([1, 2.8535162336408666]) AS v))]) AS s);
SELECT toExpHistogram(quantileDDMergeState(0.000001)(s)).scale, arrayMap(q -> round(q, 6), quantilesDDMerge(0.000001, 0.01, 0.99)(s))
FROM (SELECT arrayJoin([
    (SELECT quantileDDState(0.000001)(v) FROM (SELECT arrayJoin([1, 1.2]) AS v)),
    (SELECT quantileDDState(0.000001)(v) FROM (SELECT arrayJoin([2.8535162336408666, 1, 2.8535162336408666]) AS v))]) AS s);
-- A downscale makes the bins cover a bit less of the largest finite numbers. They go into the highest bin, whether they come
-- before the downscale or after it
SELECT a = b, a > 1.7e308 AND isFinite(a)
FROM
(
    SELECT
        (SELECT quantileDD(0.000001, 0.5)(v) FROM (SELECT arrayJoin([1.78e308, 1.78e308, 1]) AS v)) AS a,
        (SELECT quantileDD(0.000001, 0.5)(v) FROM (SELECT arrayJoin([1, 1.78e308, 1.78e308]) AS v)) AS b
);
-- States of versions 26.2 and earlier can have bins too far apart. Reading downscales them as adding the values does.
-- Cases: sparse, dense, a positive store that must downscale for the negative one, and a dense negative store read after
-- a downscale
SELECT name, (toExpHistogram(state) AS h).count, h.scale, CAST(state, 'String') = CAST(expected, 'String')
FROM
(
    WITH
        (1 + 0.000001) / (1 - 0.000001) AS gamma,
        concat(unhex('02'), reinterpretAsString(gamma), unhex('0000000000000000')) AS mapping,
        reinterpretAsString(1.) AS one,
        concat(unhex('0402'), unhex('00'), one, unhex('C09A0C'), one) AS sparse_far,
        unhex('030C000002') AS no_negative,
        unhex('040000000000000000') AS no_zeros,
        k -> exp((k + 0.5) * log(gamma)) AS value
    SELECT 'sparse' AS name,
        CAST(concat(mapping, unhex('01'), sparse_far, no_negative, no_zeros), 'AggregateFunction(quantileDD(0.000001), Float64)') AS state,
        (SELECT quantileDDState(0.000001)(value(k)) FROM (SELECT arrayJoin([0, 100000]) AS k)) AS expected
    UNION ALL
    SELECT 'dense',
        CAST(concat(mapping, unhex('010CA08D060002'), repeat(one, 100000), no_negative, no_zeros), 'AggregateFunction(quantileDD(0.000001), Float64)'),
        (SELECT quantileDDState(0.000001)(value(number)) FROM numbers(100000))
    UNION ALL
    SELECT 'negative',
        CAST(concat(mapping, unhex('01040200'), one, unhex('02'), one, unhex('03'), sparse_far, no_zeros), 'AggregateFunction(quantileDD(0.000001), Float64)'),
        (SELECT quantileDDState(0.000001)(v) FROM (SELECT arrayJoin([value(0), value(1), -value(0), -value(100000)]) AS v))
    UNION ALL
    SELECT 'negative dense',
        CAST(concat(mapping, unhex('01'), sparse_far, unhex('030C020002'), one, one, no_zeros), 'AggregateFunction(quantileDD(0.000001), Float64)'),
        (SELECT quantileDDState(0.000001)(v) FROM (SELECT arrayJoin([value(0), value(100000), -value(0), -value(1)]) AS v))
)
ORDER BY name;

SELECT 'states of quantileDD';
SELECT toExpHistogram(quantileDDState(0.01)(number)) AS h, h.count FROM numbers(10);
SELECT toExpHistogram(quantileDDState(0.01)(toFloat64(number) - 5)) FROM numbers(10);
SELECT toExpHistogram(quantileDDState(0.01)(number), 3) FROM numbers(10);
SELECT toExpHistogram(quantilesDDState(0.01, 0.5)(toInt8(number))) FROM numbers(10);
SELECT toExpHistogram(medianDDState(0.01)(toDate('2020-01-01') + number)).scale FROM numbers(10);
SELECT h.scale, h.count, arraySum(h.positive_bucket_counts) + h.zero_count
FROM (SELECT toExpHistogram(quantileDDState(0.001)(number * 1.5)) AS h FROM numbers(100000));
-- Quantiles survive the round trip through the exponential histogram within about twice the accuracy of the state
SELECT arrayMap(
    (a, b) -> abs(a - b) / b < 0.02 + 1e-9,
    (SELECT quantilesDDMerge(0.0001, 0.1, 0.5, 0.9)(fromExpHistogram(h.scale, h.zero_count, h.positive_offset, h.positive_bucket_counts, h.negative_offset, h.negative_bucket_counts))
     FROM (SELECT toExpHistogram(quantileDDState(0.01)(number + 1)) AS h FROM numbers(100000))),
    (SELECT quantilesDD(0.01, 0.1, 0.5, 0.9)(number + 1) FROM numbers(100000)));

SELECT toExpHistogram(quantileDDIfState(0.01)(number, number > 2)) FROM numbers(10);

SELECT 'states finer than scale 20';
-- A state from another producer, with gamma = 1.0000001 (finer than scale 22) and 3 values in bin 1000
-- `fromExpHistogram` accepts the result, and stores scale 20 as 18
WITH unhex(concat('02', hex(reinterpretAsFixedString(toFloat64(1.0000001))), '0000000000000000',
    '010C01D00F02', hex(reinterpretAsFixedString(toFloat64(3))), '030C000002', '04', '0000000000000000'))::AggregateFunction(quantileDD(0.01), Float64) AS state,
    toExpHistogram(state) AS h
SELECT h, toExpHistogram(fromExpHistogram(h.scale, h.zero_count, h.positive_offset, h.positive_bucket_counts, h.negative_offset, h.negative_bucket_counts));
SELECT toExpHistogram(unhex(concat('02', hex(reinterpretAsFixedString(toFloat64(1.0000001))), '0000000000000000',
    '010C01D00F02', hex(reinterpretAsFixedString(toFloat64(3))), '030C000002', '04', '0000000000000000'))::AggregateFunction(quantileDD(0.01), Float64), 21); -- { serverError BAD_ARGUMENTS }

SELECT 'gammas of the scales';
-- The bits of the gamma of each scale, after the flag byte of the state. They are the same on every platform
SELECT s, hex(reverse(substring(CAST(fromExpHistogram(s, 1, 0, [], 0, []), 'String'), 2, 8))) FROM (SELECT arrayJoin(range(-5, 19)) AS s);

-- A gamma an ulp off the base of scale 10, as from another producer, is scale 10 for `toExpHistogram` and for merges.
-- Two ulps above it is not for either of them: the merges downscale to scale 9, and `toExpHistogram` reports scale 9
WITH CAST(fromExpHistogram(10, 0, 0, [1, 2, 3, 4], 0, [], 0.000001), 'String') AS canonical,
    CAST(concat(substring(canonical, 1, 1), reinterpretAsFixedString(reinterpretAsUInt64(substring(canonical, 2, 8)) + ulps), substring(canonical, 10)), 'AggregateFunction(quantileDD(0.000001), Float64)') AS off,
    fromExpHistogram(10, 0, 0, [5, 6], 0, [], 0.000001) AS scale_10,
    fromExpHistogram(9, 0, 0, [5, 6], 0, [], 0.000001) AS scale_9
SELECT ulps, toExpHistogram(off),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [off, scale_10])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [scale_10, off])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [off, scale_9])),
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [scale_9, off]))
FROM (SELECT arrayJoin([-1, 1, 2]) AS ulps);

-- Downscaled, a state an ulp off scale 10 gets the gamma of scale 9 exactly, whatever the order of the merge, and then
-- merges with a state of scale 9 without losing resolution
WITH CAST(fromExpHistogram(10, 0, 0, [1, 2, 3, 4], 0, [], 0.000001), 'String') AS canonical,
    CAST(concat(substring(canonical, 1, 1), reinterpretAsFixedString(reinterpretAsUInt64(substring(canonical, 2, 8)) + ulps), substring(canonical, 10)), 'AggregateFunction(quantileDD(0.000001), Float64)') AS off,
    fromExpHistogram(10, 0, 70000, [1], 0, [], 0.000001) AS far,
    hex(reverse(substring(CAST(fromExpHistogram(9, 1, 0, [], 0, []), 'String'), 2, 8))) AS scale_9_bits,
    toExpHistogram(arrayReduce('quantileDDMergeState(0.000001)', [arrayReduce('quantileDDMergeState(0.000001)', [off, far]), fromExpHistogram(9, 0, 0, [5], 0, [], 0.000001)])) AS h
SELECT ulps,
    hex(reverse(substring(CAST(arrayReduce('quantileDDMergeState(0.000001)', [off, far]), 'String'), 2, 8))) = scale_9_bits,
    hex(reverse(substring(CAST(arrayReduce('quantileDDMergeState(0.000001)', [far, off]), 'String'), 2, 8))) = scale_9_bits,
    h.count, h.scale, h.positive_offset, arraySlice(h.positive_bucket_counts, 1, 3), length(h.positive_bucket_counts)
FROM (SELECT arrayJoin([-1, 1]) AS ulps);

-- Likewise at scale 0, whose gamma 2 is a power of two: an ulp off is scale 0, two ulps above it is not
WITH CAST(fromExpHistogram(0, 0, 0, [1, 2, 3, 4], 0, [], 0.000001), 'String') AS canonical,
    CAST(concat(substring(canonical, 1, 1), reinterpretAsFixedString(reinterpretAsUInt64(substring(canonical, 2, 8)) + ulps), substring(canonical, 10)), 'AggregateFunction(quantileDD(0.000001), Float64)') AS off
SELECT ulps, toExpHistogram(off).scale
FROM (SELECT arrayJoin([-1, 1, 2]) AS ulps);

SELECT 'errors';
SELECT fromExpHistogram(-6, 0, 0, [1], 0, []); -- { serverError BAD_ARGUMENTS }
SELECT fromExpHistogram(21, 0, 0, [1], 0, []); -- { serverError BAD_ARGUMENTS }
SELECT fromExpHistogram(0, 0, toInt64(9223372036854775807), [1, 1], 0, []); -- { serverError BAD_ARGUMENTS }
SELECT fromExpHistogram(0, 0, 2000000000, [1], 0, []); -- { serverError BAD_ARGUMENTS }
-- Buckets beyond the finite numbers
SELECT fromExpHistogram(0, 0, -1076, [1], 0, []); -- { serverError BAD_ARGUMENTS }
SELECT fromExpHistogram(0, 0, 0, [], 1024, [1]); -- { serverError BAD_ARGUMENTS }
SELECT fromExpHistogram(20, 0, 1073741824, [1], 0, []); -- { serverError BAD_ARGUMENTS }
SELECT fromExpHistogram(0, 0, 0, [1], 0); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromExpHistogram(0, 0, 0, [-1], 0, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(0, -1, 0, [1], 0, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(toUInt64(0), 0, 0, [1], 0, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(0, 0, 0, [1], 0, [], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(0, 0, 0, [1], 0, [], toFloat32(0.01)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromExpHistogram(0, 0, 0, [1], 0, [], materialize(0.01)); -- { serverError ILLEGAL_COLUMN }
SELECT fromExpHistogram(0, 0, 0, [1], 0, [], 1.5); -- { serverError BAD_ARGUMENTS }
SELECT toExpHistogram(quantileDDState(0.01)(number), 6) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT toExpHistogram(fromExpHistogram(0, 0, 0, [1], 0, []), -6); -- { serverError BAD_ARGUMENTS }
SELECT toExpHistogram(quantileDDState(0.9999999999)(number)) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT toExpHistogram(quantileTDigestState(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toExpHistogram(quantileDDState(0.01)(toNullable(number))) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toExpHistogram(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toExpHistogram(fromExpHistogram(0, 0, 0, [1], 0, []), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toExpHistogram(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
