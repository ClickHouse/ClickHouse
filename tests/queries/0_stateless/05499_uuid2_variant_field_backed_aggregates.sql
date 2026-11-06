-- Aggregate functions that keep the values of their argument in a `Field` lose the alternative of a `Variant` value.
-- When the `Variant` has both a `UUID` and a `UUID2` alternative, inserting the value back could choose the other
-- alternative and change the identifier, so such argument types are rejected.

SET allow_suspicious_variant_types = 1;

SELECT any_respect_nulls(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT anyLast_respect_nulls(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupArrayInsertAt(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2)), toUInt64(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupArraySorted(3)(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumMap([CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2))], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT any_respect_nulls(CAST([toUUID2('00000000-0000-0000-0000-000000000001')] AS Variant(Array(UUID1), Array(UUID2)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT any_respect_nulls(tuple(CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The column-backed state of `any` keeps the alternative.
SELECT any(v), variantType(any(v)) FROM (SELECT CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2)) AS v);

-- A single `UUID` type among the alternatives, or both of them in one alternative, is not ambiguous.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    any_respect_nulls(CAST(toUUID2(s) AS Variant(UUID2, String))),
    anyLast_respect_nulls(CAST(toUUID(s) AS Variant(UUID1, String))),
    groupArrayInsertAt(CAST(toUUID2(s) AS Variant(UUID2, String)), toUInt64(1)),
    groupArraySorted(3)(CAST(toUUID2(s) AS Variant(UUID2, String))),
    sumMap([CAST(toUUID2(s) AS Variant(UUID2, String))], [1]),
    any_respect_nulls(CAST(tuple(toUUID(s), toUUID2(s)) AS Variant(Tuple(UUID1, UUID2), String)));
