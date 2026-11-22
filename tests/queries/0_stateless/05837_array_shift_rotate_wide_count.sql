WITH [1, 2, 3, 4, 5] AS a, toUInt64('18446744073709551615') AS n
SELECT arrayShiftLeft(a, n, 9), arrayShiftRight(a, n, 9), arrayRotateLeft(a, n), arrayRotateRight(a, n);

WITH [1, 2, 3, 4, 5] AS a, materialize(toUInt64('9223372036854775808')) AS n
SELECT arrayShiftLeft(a, toNullable(n)), arrayRotateLeft(a, n), arrayRotateRight(a, n);

SELECT arrayShiftLeft([1, 2, 3], toInt64(-1)), arrayRotateRight([1, 2, 3], toInt64(-1));
