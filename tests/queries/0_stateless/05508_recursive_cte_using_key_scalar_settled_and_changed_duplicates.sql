-- Scalar subqueries and EXISTS are evaluated once during analysis, while the tables of a `USING KEY`
-- recursive CTE are still empty, so reading them there is rejected instead of folding a wrong constant.

SET allow_experimental_keyed_recursive_cte = 1;

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM t
    WHERE k < 3 AND (SELECT count() FROM t_settled) > 0
)
SELECT * FROM t ORDER BY k; -- { serverError UNSUPPORTED_METHOD }

-- `EXISTS` is folded during analysis only with this setting (CI randomizes it); without it the
-- subquery is evaluated at every step like `IN` below.
SET execute_exists_as_scalar_subquery = 1;

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM t
    WHERE k < 3 AND EXISTS (SELECT k FROM t_settled)
)
SELECT * FROM t ORDER BY k; -- { serverError UNSUPPORTED_METHOD }

WITH RECURSIVE t USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM t
    WHERE k < 3 AND (SELECT count() FROM t) > 0
)
SELECT * FROM t ORDER BY k; -- { serverError UNSUPPORTED_METHOD }

-- IN reads the settled table at every iteration.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT 1 AS k
    UNION ALL
    SELECT k + 1 FROM t
    WHERE k < 3 AND k IN (SELECT k FROM t_settled)
)
SELECT * FROM t ORDER BY k;

-- Rows superseded within a step by a later changed row with the same key are reclaimed while the step
-- is collected, so the memory of the step is bounded by the distinct keys rather than by the produced rows.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(1) AS k, repeat('x', 1024) AS v
    UNION ALL
    SELECT k, repeat('y', 1024)
    FROM t CROSS JOIN numbers(200000) AS n
    WHERE v = repeat('x', 1024)
)
SELECT k, length(v), substring(v, 1, 1) FROM t
SETTINGS max_threads = 1, max_block_size = 1024, max_joined_block_size_rows = 1024, max_memory_usage = 67108864;

-- Last-wins semantics are preserved by the compaction: the last produced row per key survives.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT toUInt64(number) AS k, toUInt64(0) AS v FROM numbers(3)
    UNION ALL
    SELECT k, n.number + 1
    FROM (SELECT * FROM t WHERE v = 0) AS t CROSS JOIN (SELECT number FROM numbers(10000) ORDER BY number) AS n
    ORDER BY n.number, k
)
SELECT k, v FROM t ORDER BY k
SETTINGS max_threads = 1, max_block_size = 128;
