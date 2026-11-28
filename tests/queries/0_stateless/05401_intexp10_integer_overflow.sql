-- Regression test for #125945: `intExp10` narrowed integer inputs to `int`
-- before range-checking, so values with magnitude >= 2^31 returned nonsense.
-- After the fix, integer inputs saturate the same way `intExp2` does (see #105054).

-- Literal 4294967297 is UInt64 in ClickHouse; before the fix static_cast<int> took
-- the low 32 bits and returned 10, now it saturates.
SELECT intExp10(4294967297);

-- Literal 2147483648 is UInt32; before the fix it truncated to INT_MIN and returned 0,
-- now it saturates.
SELECT intExp10(2147483648);

-- Integer casts with magnitude > INT_MAX
SELECT intExp10(toInt64(4294967297)); -- saturates
SELECT intExp10(toInt64(-4294967297)); -- returns 0
SELECT intExp10(toInt64(9223372036854775807)); -- INT64_MAX saturates
SELECT intExp10(toInt64(-9223372036854775807)); -- returns 0
SELECT intExp10(toUInt64(18446744073709551611)); -- saturates
SELECT intExp10(toUInt32(4294967295)); -- saturates

-- Behavior at normal boundaries is unchanged
SELECT intExp10(19); -- 10000000000000000000
SELECT intExp10(20); -- saturates
SELECT intExp10(-5); -- 0
SELECT intExp10(15); -- 1000000000000000
SELECT intExp10(toInt32(15)); -- 1000000000000000

-- Float path unchanged (checked before the range checks, same as intExp2)
SELECT intExp10(19.5); -- 10000000000000000000
SELECT intExp10(20.5); -- saturates
SELECT intExp10(-5.0); -- 0
