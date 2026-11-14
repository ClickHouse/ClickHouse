-- A serialized `uniqCombined` medium set (hash table) must not claim more elements
-- than the estimator ever keeps there, otherwise a few corrupted bytes request a huge allocation.

-- Medium set (container type 2) claiming 2^30 elements with no elements following.
SELECT finalizeAggregation(CAST(unhex('028080808004') AS AggregateFunction(uniqCombined64(12), UInt64))); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT finalizeAggregation(CAST(unhex('028080808004') AS AggregateFunction(uniqCombined(12), UInt64))); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- A medium set filled up to its maximum size still round-trips.
-- For precision 12 the maximum is 128 elements for `uniqCombined64` and 256 for `uniqCombined`.
SELECT finalizeAggregation(CAST(CAST(uniqCombined64State(12)(number) AS String) AS AggregateFunction(uniqCombined64(12), UInt64))) FROM numbers(128);
SELECT finalizeAggregation(CAST(CAST(uniqCombinedState(12)(number) AS String) AS AggregateFunction(uniqCombined(12), UInt64))) FROM numbers(256);
SELECT hex(substring(CAST(uniqCombined64State(12)(number) AS String), 1, 1)) FROM numbers(128);
SELECT hex(substring(CAST(uniqCombinedState(12)(number) AS String), 1, 1)) FROM numbers(256);
