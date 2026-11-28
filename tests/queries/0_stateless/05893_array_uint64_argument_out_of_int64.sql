-- A UInt64 size of 2^63 or more in arrayResize must not be read as negative.
SELECT arrayResize([1, 2], 18446744073709551612); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize([1, 2], 18446744073709551612, 7); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize([1, 2, 3], 9223372036854775808); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize(materialize([1, 2]), 18446744073709551612); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize([1, 2], materialize(toUInt64(18446744073709551612))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize(materialize([1, 2]), materialize(toUInt64(18446744073709551612))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize([1, 2], toLowCardinality(toUInt64(18446744073709551612))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT arrayResize([1, 2], 9223372036854775807); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayResize([1, 2], 4), arrayResize([1, 2], -4), arrayResize([1, 2, 3], -1), arrayResize([1, 2], toUInt64(3), 7);
SELECT arrayResize(materialize([1, 2]), materialize(toUInt64(number))) FROM numbers(3);

-- A UInt64 index of arrayReduceInRanges of 2^63 or more is after the end of the array.
SELECT arrayReduceInRanges('sum', [(18446744073709551615, 1)], [1, 2, 3]);
SELECT arrayReduceInRanges('sum', [(9223372036854775808, 18446744073709551615), (2, 1), (1, 3)], [1, 2, 3]);
SELECT arrayReduceInRanges('sum', materialize([(18446744073709551615, 1), (3, 1)]), materialize([1, 2, 3]));
SELECT arrayReduceInRanges('sum', [(toUInt64(18446744073709551615), 1)], range(200));
SELECT arrayReduceInRanges('sum', [(-1, 1), (-3, 2)], [1, 2, 3]);

-- clear_depth of arrayEnumerateUniqRanked and arrayEnumerateDenseRanked must not be truncated to 32 bits.
SELECT arrayEnumerateUniqRanked(4294967297, [[1, 2], [1, 3]]); -- { serverError BAD_ARGUMENTS }
SELECT arrayEnumerateUniqRanked(4294967298, [[1, 2], [1, 3]]); -- { serverError BAD_ARGUMENTS }
SELECT arrayEnumerateDenseRanked(4294967298, [[1, 2], [1, 3]]); -- { serverError BAD_ARGUMENTS }
SELECT arrayEnumerateUniqRanked(1, [[1, 2], [1, 3]]), arrayEnumerateUniqRanked(2, [[1, 2], [1, 3]]), arrayEnumerateDenseRanked(2, [[1, 2], [1, 3]]);
