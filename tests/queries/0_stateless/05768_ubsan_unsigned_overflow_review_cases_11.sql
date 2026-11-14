-- Malformed aggregate states that previously reached integer overflow or an invalid conversion (which UBSan reports).

-- Valid `quantileGK` states still deserialize and merge, including the largest `delta` of accuracy 1.
SELECT quantileGK(1, 0.5)(number), quantileGK(100, 0.5)(number), quantileGK(10000, 0.9)(number) FROM numbers(100000)
SETTINGS max_threads = 1;
SELECT quantileGKMerge(1, 0.5)(st), quantileGKMerge(1, 0.5)(st2) FROM (
    SELECT CAST(unhex(hex(quantileGKState(1, 0.5)(number))), 'AggregateFunction(quantileGK(1, 0.5), UInt64)') AS st,
        quantileGKState(1, 0.5)(number) AS st2
    FROM numbers(100000) GROUP BY number % 3)
SETTINGS max_threads = 1;
SELECT quantileGKMerge(100, 0.5)(st) FROM (
    SELECT CAST(unhex(hex(quantileGKState(100, 0.5)(number))), 'AggregateFunction(quantileGK(100, 0.5), UInt64)') AS st
    FROM numbers(100000) GROUP BY number % 3)
SETTINGS max_threads = 1;

-- Merged states round-trip too.
SELECT quantileGKMerge(1, 0.5)(m), quantileGKMerge(100, 0.5)(m2) FROM (
    SELECT CAST(unhex(hex(quantileGKMergeState(1, 0.5)(st))), 'AggregateFunction(quantileGK(1, 0.5), UInt64)') AS m,
        CAST(unhex(hex(quantileGKMergeState(100, 0.5)(st2))), 'AggregateFunction(quantileGK(100, 0.5), UInt64)') AS m2
    FROM (SELECT number % 5 AS k, quantileGKState(1, 0.5)(number) AS st, quantileGKState(100, 0.5)(number) AS st2
        FROM numbers(100000) GROUP BY number % 15, k)
    GROUP BY k)
SETTINGS max_threads = 1;

-- A `quantileGK` sample with `g` of `INT64_MAX`.
SELECT finalizeAggregation(CAST(
    unhex('10270000000000007b14ae47e17a843f020000000000000001000000000000000100000000000000ffffffffffffff7f0100000000000000')
    AS AggregateFunction(quantileGK(100), UInt64))); -- { serverError INCORRECT_DATA }

-- A NaN bin of `quantileBFloat16` with an integer result.
SELECT quantileBFloat16(0.5)(toDate(number)) FROM numbers(100);
SELECT finalizeAggregation(CAST(unhex('01c07f0100000000000000') AS AggregateFunction(quantileBFloat16, Date))); -- { serverError INCORRECT_DATA }
