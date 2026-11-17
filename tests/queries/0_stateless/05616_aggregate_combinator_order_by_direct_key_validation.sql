-- The sort keys of the directly called `OrderBy` combinator are validated as in the sugared `f(x ORDER BY k)` form.

SET enable_analyzer = 1;

-- `Variant` and `Dynamic` keys are rejected unless `allow_suspicious_types_in_order_by` is set.
SET allow_suspicious_types_in_order_by = 0;
SELECT groupArrayOrderBy('0 ASC')(x, v) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
SELECT groupArrayOrderBy('0 ASC')(x, d) FROM (SELECT number AS x, number::Dynamic AS d FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
SELECT groupArrayOrderBy('0 ASC, 1 ASC')(x, x, v) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
-- The combinator is found under other combinators as well.
SELECT groupArrayOrderByIf('0 ASC')(x, v, x > 0) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
-- And in window functions.
SELECT groupArrayOrderBy('0 ASC')(x, v) OVER () FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }

-- The nested arguments are not sort keys.
SELECT groupArrayOrderBy('0 DESC')(v, x) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3));

SET allow_suspicious_types_in_order_by = 1;
SELECT groupArrayOrderBy('0 DESC')(x, v) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3));

-- Non-comparable keys are rejected.
SELECT groupArrayOrderBy('0 ASC')(x, m) FROM (SELECT number AS x, arrayReduce('uniqState', [number]) AS m FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
