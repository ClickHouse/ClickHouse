-- A `NaN` member of a literal `IN` set cannot equal any integer: it is left out of the set, the other members are
-- kept. The mixed-type list makes the set convert its members one by one.

SELECT toInt32(0) IN (nan, 0);
SELECT toInt32(0) NOT IN (nan, 1);
SELECT toInt32(0) IN (nan);
SELECT toUInt8(0) IN (toFloat32(nan), 0);
SELECT toInt128(0) IN (nan, 0);
SELECT toInt64(0) IN (-nan, 1, 0);
SELECT count() FROM numbers(5) WHERE toInt32(number) IN (nan, 1, 3);
