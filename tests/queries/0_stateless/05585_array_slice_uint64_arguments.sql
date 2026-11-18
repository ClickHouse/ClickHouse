-- Tags: no-fasttest
-- arraySlice reads its integer arguments with getInt(), so a UInt64 value of 2^63 or more used to
-- turn negative and trigger the "count from the end" semantics instead of being reported.

SELECT arraySlice([1, 2, 3], 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arraySlice([1, 2, 3], 18446744073709551615, 1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arraySlice([1, 2, 3], 1, 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- A non-constant argument reaches the same guard.
SELECT arraySlice(materialize([1, 2, 3]), materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- `arraySlice` handles NULLs itself, so a `Nullable` argument is checked too, ignoring the value under a NULL.
SELECT arraySlice([1, 2, 3], toNullable(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arraySlice([1, 2, 3], nullIf(materialize(toUInt64(18446744073709551615)), 18446744073709551615));
SELECT arraySlice([1, 2, 3], 1, nullIf(materialize(toUInt64(18446744073709551615)), 18446744073709551615));

-- In-range behaviour is unchanged, including the negative-argument meanings.
SELECT arraySlice([1, 2, 3], 2);
SELECT arraySlice([1, 2, 3], -1);
SELECT arraySlice([1, 2, 3], 1, -1);
SELECT arraySlice([1, 2, 3], toNullable(toUInt64(2)));
