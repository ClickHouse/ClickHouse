-- Covers the two-way summary merge of ApproxSampler::merge in
-- src/AggregateFunctions/AggregateFunctionQuantileGK.cpp. It is reached only through the
-- `-Merge` combinator of quantileGK once more than one aggregate state is combined; a single
-- state only exercises the `count == 0` fast path.
--
-- Expected behaviour (FunctionDocumentation of quantileGK in that file): `accuracy` is a
-- "Constant positive integer. Larger accuracy value means less error." With accuracy = 10000
-- the relative error is 1e-4, so for the few hundred values used below the Greenwald-Khanna
-- summary discards no sample (the compression threshold 2 * 1e-4 * count stays far below 1,
-- while every sample contributes at least 1 to the compared rank jump) and every rank span
-- stays 0. The merged median is therefore the exact ceil(0.5 * count)-th smallest value of the
-- union of both states, independent of the merge order, the block size and the thread count.

-- 1. One of the two states is empty, so the merge returns at once on `other.count == 0`.
--    The union is {0, ..., 399}: 400 values, the 200th smallest is 199.
SELECT 'empty state', quantileGKMerge(10000, 0.5)(st)
FROM
(
    SELECT quantileGKState(10000, 0.5)(number) AS st FROM numbers(400)
    UNION ALL
    SELECT quantileGKState(10000, 0.5)(number) AS st FROM numbers(0)
);

-- 2. Fully interleaved states of unequal size, so the two-list merge alternates sides on every
--    iteration and adjusts the rank spans of both sides.
--    The union is {0, 2, ..., 398} + {1, 3, ..., 299}: 350 values, of which 0..299 are the 300
--    smallest, so the 175th smallest is 174.
SELECT 'interleaved states', quantileGKMerge(10000, 0.5)(st)
FROM
(
    SELECT quantileGKState(10000, 0.5)(number * 2) AS st FROM numbers(200)
    UNION ALL
    SELECT quantileGKState(10000, 0.5)(number * 2 + 1) AS st FROM numbers(150)
);

-- 3. Disjoint states with the greater values in the second branch, so the two-list merge
--    consumes one side completely and a "copy the remaining samples" loop drains the other.
--    The union is {0, ..., 299} + {1000, ..., 1049}: 350 values, the 175th smallest is 174.
SELECT 'disjoint states, greater part second', quantileGKMerge(10000, 0.5)(st)
FROM
(
    SELECT quantileGKState(10000, 0.5)(number) AS st FROM numbers(300)
    UNION ALL
    SELECT quantileGKState(10000, 0.5)(number + 1000) AS st FROM numbers(50)
);

-- 4. The same two states with the branches swapped, so the merge drains the opposite side. The
--    union is unchanged, so the median stays the value of statement 3.
SELECT 'disjoint states, greater part first', quantileGKMerge(10000, 0.5)(st)
FROM
(
    SELECT quantileGKState(10000, 0.5)(number + 1000) AS st FROM numbers(50)
    UNION ALL
    SELECT quantileGKState(10000, 0.5)(number) AS st FROM numbers(300)
);
