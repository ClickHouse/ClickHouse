-- The bucket for an operand above the range is `count + 1`. It was computed in the type of the count and wrapped around
-- to 0, the bucket for an operand below the range, for the maximum `UInt32` count, although the result type is `UInt64`.
SELECT widthBucket(10, 0, 5, toUInt32(4294967295)), toTypeName(widthBucket(10, 0, 5, toUInt32(4294967295)));
SELECT widthBucket(materialize(10), 0, 5, toUInt32(4294967295));
SELECT widthBucket(10, 0, 5, toUInt8(255)), widthBucket(10, 0, 5, toUInt16(65535));

-- For a `UInt64` count, the result type is also `UInt64`, so the bucket does not fit for the maximum count.
SELECT widthBucket(10, 0, 5, toUInt64(18446744073709551615)); -- { serverError BAD_ARGUMENTS }
SELECT widthBucket(materialize(10), 0, 5, toUInt64(18446744073709551615)); -- { serverError BAD_ARGUMENTS }
SELECT widthBucket(-1, 0, 5, toUInt64(18446744073709551615));
SELECT widthBucket(10, 0, 5, toUInt64(18446744073709551614));
