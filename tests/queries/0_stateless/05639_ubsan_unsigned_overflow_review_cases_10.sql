-- Tags: no-fasttest
-- no-fasttest: ORC is not available in the fast test build.

-- Boundary cases that previously wrapped around an unsigned integer (which UBSan now reports).

-- `windowFunnel` keeps the type of wide timestamps instead of narrowing them to `UInt64`.
SELECT windowFunnel(1)(toUInt128('18446744073709551616') + number, number = 0, number = 1) FROM numbers(2) SETTINGS max_threads = 1;
SELECT windowFunnel(1, 'strict_once')(toUInt128('18446744073709551616') + number, number = 0, number = 1) FROM numbers(2) SETTINGS max_threads = 1;
SELECT windowFunnel(1)(toUInt256('18446744073709551616') + number * 2, number = 0, number = 1) FROM numbers(2) SETTINGS max_threads = 1;

-- An Npy row with no data bytes but too many offsets is rejected.
SELECT * FROM format(Npy, 'x Array(Array(UInt8))',
    unhex('934e554d5059010048007b276465736372273a20277c7531272c2027666f727472616e5f6f72646572273a2066616c73652c20277368617065273a2028312c20313037333734313832342c2030292c207d0a')); -- { serverError INCORRECT_DATA }

-- A first LZO command below 17 in an ORC footer is malformed.
SELECT * FROM format(ORC, 'x UInt8', unhex('4f5243020000100804100304')); -- { serverError INCORRECT_DATA }
