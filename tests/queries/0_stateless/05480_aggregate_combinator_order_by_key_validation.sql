-- The sort keys of the `ORDER BY` combinator are validated as in the regular `ORDER BY`.

SET enable_analyzer = 1;

-- A matcher that expands to a single column is unwrapped to that column.
SELECT groupArray(x ORDER BY *) FROM (SELECT number AS x FROM numbers(5) ORDER BY x DESC);
SELECT groupArray(x ORDER BY * DESC) FROM (SELECT number AS x FROM numbers(5));

-- A matcher that expands to several columns is rejected.
SELECT groupArray(x ORDER BY *) FROM (SELECT number AS x, number AS y FROM numbers(5)); -- { serverError BAD_ARGUMENTS }

-- `Variant` and `Dynamic` keys are rejected unless `allow_suspicious_types_in_order_by` is set.
SET allow_suspicious_types_in_order_by = 0;
SELECT groupArray(x ORDER BY v) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
SELECT groupArray(x ORDER BY d) FROM (SELECT number AS x, number::Dynamic AS d FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }

SET allow_suspicious_types_in_order_by = 1;
SELECT groupArray(x ORDER BY v DESC) FROM (SELECT number AS x, number::Variant(UInt64, String) AS v FROM numbers(3));

-- Non-comparable keys are rejected.
SELECT groupArray(x ORDER BY m) FROM (SELECT number AS x, arrayReduce('uniqState', [number]) AS m FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
