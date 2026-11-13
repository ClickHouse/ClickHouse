-- The null-safe comparison of `Nullable` tuples with a `FixedString` compares the `FixedString` zero-padded,
-- and the other elements as without a `FixedString`, where two NaNs are not distinct.

SELECT
    isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2), nan)), toNullable(tuple('a', nan))),
    isNotDistinctFrom(toNullable(tuple('a', nan)), toNullable(tuple('a', nan))),
    isDistinctFrom(toNullable(tuple(toFixedString('a', 2), nan)), toNullable(tuple('a', nan))),
    isDistinctFrom(toNullable(tuple('a', nan)), toNullable(tuple('a', nan)));

SELECT
    isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2), 1)), toNullable(tuple('b', 1))),
    isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2), 1)), toNullable(tuple('a\0', 1))),
    isNotDistinctFrom(toNullable(tuple(toFixedString('a', 2), 1)), CAST(NULL, 'Nullable(Tuple(String, UInt8))')),
    isNotDistinctFrom(CAST(NULL, 'Nullable(Tuple(FixedString(2), UInt8))'), CAST(NULL, 'Nullable(Tuple(String, UInt8))'));

SELECT
    isNotDistinctFrom(toNullable(tuple(tuple(toFixedString('a', 2), nan), toNullable(toFixedString('x', 1)))), toNullable(tuple(tuple('a', nan), CAST(NULL, 'Nullable(String)')))),
    isNotDistinctFrom(toNullable(tuple(tuple(toFixedString('a', 2), nan), CAST(NULL, 'Nullable(FixedString(1))'))), toNullable(tuple(tuple('a', nan), CAST(NULL, 'Nullable(String)')))),
    isDistinctFrom(toNullable(tuple(tuple(toFixedString('a', 2), nan), CAST(NULL, 'Nullable(FixedString(1))'))), toNullable(tuple(tuple('a', nan), CAST(NULL, 'Nullable(String)'))));

SELECT x, isNotDistinctFrom(toNullable(tuple(toFixedString(x, 2), nan)), toNullable(tuple(toLowCardinality('a'), nan)))
FROM (SELECT arrayJoin(['a', 'b']) AS x) ORDER BY x;
