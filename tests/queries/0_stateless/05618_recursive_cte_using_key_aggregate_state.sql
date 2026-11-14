-- A produced row is propagated only when it differs from the accumulated row of its key, and aggregate
-- function states cannot be compared for equality (`ColumnAggregateFunction::compareAt` reports every
-- pair of states as equal), so a changed state would be dropped and the recursion would stop early with
-- a wrong result. Such column types are rejected, including when nested in a container type.

SET allow_experimental_keyed_recursive_cte = 1;

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, sumState(toUInt64(1)) AS s
    UNION ALL
    SELECT k, sumState(finalizeAggregation(s) + toUInt64(1))
    FROM t
    WHERE finalizeAggregation(s) < 3
    GROUP BY k
)
SELECT k, finalizeAggregation(s) FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, tuple(sumState(toUInt64(1))) AS s
    UNION ALL
    SELECT k, s FROM t WHERE k < 1
)
SELECT k FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, [sumState(toUInt64(1))] AS s
    UNION ALL
    SELECT k, s FROM t WHERE k < 1
)
SELECT k FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `Variant` reports itself comparable regardless of its variants, also when nested in a container type.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, CAST(tuple(initializeAggregation('sumState', number + 1)), 'Tuple(Variant(AggregateFunction(sum, UInt64), String))') AS s FROM numbers(1)
    UNION ALL
    SELECT k, s FROM t WHERE k < 1
)
SELECT k FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, CAST([initializeAggregation('sumState', number + 1)], 'Array(Variant(AggregateFunction(sum, UInt64), String))') AS s FROM numbers(1)
    UNION ALL
    SELECT k, s FROM t WHERE k < 1
)
SELECT k FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `Dynamic` and `JSON` hold values of types that are only known at run time, aggregate states included.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, CAST(initializeAggregation('sumState', number + 1), 'Dynamic') AS s FROM numbers(1)
    UNION ALL
    SELECT k, CAST(initializeAggregation('sumState', finalizeAggregation(dynamicElement(s, 'AggregateFunction(sum, UInt64)')) + toUInt64(1)), 'Dynamic')
    FROM t
    WHERE finalizeAggregation(dynamicElement(s, 'AggregateFunction(sum, UInt64)')) < 3
)
SELECT k, finalizeAggregation(dynamicElement(s, 'AggregateFunction(sum, UInt64)')) FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, CAST(concat('{"a":', toString(number), '}'), 'JSON') AS s FROM numbers(1)
    UNION ALL
    SELECT k, s FROM t WHERE k < 1
)
SELECT k FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same computation over a plain value reaches the fixpoint.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, toUInt64(1) AS s
    UNION ALL
    SELECT k, s + 1 FROM t WHERE s < 3
)
SELECT k, s FROM t;
