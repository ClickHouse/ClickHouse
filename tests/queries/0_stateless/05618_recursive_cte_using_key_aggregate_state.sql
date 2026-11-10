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

-- The same computation over a plain value reaches the fixpoint.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, toUInt64(1) AS s
    UNION ALL
    SELECT k, s + 1 FROM t WHERE s < 3
)
SELECT k, s FROM t;
