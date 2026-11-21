-- A `Dynamic` NULL inside a nullable tuple compared with a `FixedString` NULL is not distinct from it.
SET allow_experimental_dynamic_type = 1;

SELECT
    isNotDistinctFrom(toNullable(tuple(NULL::Nullable(FixedString(2)))), toNullable(tuple(NULL::Dynamic))),
    isDistinctFrom(toNullable(tuple(NULL::Nullable(FixedString(2)))), toNullable(tuple(NULL::Dynamic)));

SELECT
    isNotDistinctFrom(toNullable(tuple(NULL::Dynamic)), toNullable(tuple(NULL::Nullable(FixedString(2))))),
    isDistinctFrom(toNullable(tuple(NULL::Dynamic)), toNullable(tuple(NULL::Nullable(FixedString(2)))));

-- Only one side is NULL.
SELECT
    isNotDistinctFrom(toNullable(tuple(toNullable(toFixedString('a', 2)))), toNullable(tuple(NULL::Dynamic))),
    isDistinctFrom(toNullable(tuple(toNullable(toFixedString('a', 2)))), toNullable(tuple(NULL::Dynamic)));

SELECT
    isNotDistinctFrom(toNullable(tuple(NULL::Dynamic)), toNullable(tuple(toNullable(toFixedString('a', 2))))),
    isDistinctFrom(toNullable(tuple(NULL::Dynamic)), toNullable(tuple(toNullable(toFixedString('a', 2)))));
