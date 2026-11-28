-- Boundary cases that previously reached an undefined conversion or a wrap-around (which UBSan reports).

-- A total weight close to the maximum of `UInt64` rounds up to 2^64 as `Float64`: the rank saturates, and the last
-- rank is the second value.
SELECT quantileExactWeightedInterpolated(1)(x, w), quantileExactWeightedInterpolated(0.5)(x, w),
    quantilesExactWeightedInterpolated(0, 0.5, 1)(x, w)
FROM values('x UInt64, w UInt64', (1, 18446744073709551614), (2, 1));
SELECT quantileExactWeightedInterpolated(0.5)(x, w) FROM values('x UInt64, w UInt64', (1, 3), (2, 1));

-- An Npy row of 2^30 empty strings takes 8 GiB of `String` offsets, over the limit of a row.
SELECT length(x) FROM format(Npy, 'x Array(String)', unhex('934e554d5059010076007b276465736372273a20277c5330272c2027666f727472616e5f6f72646572273a2046616c73652c20277368617065273a2028312c2031303733373431383234292c207d202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020200a')); -- { serverError INCORRECT_DATA }
