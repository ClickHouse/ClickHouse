SET enable_analyzer = 1;
SET allow_experimental_keyed_recursive_cte = 1;

-- Competing paths to one node within the same step: both candidates for node 4 pass the
-- `<cte_name>_settled` predicate, and `ORDER BY cost DESC` makes the cheapest one the last produced one.
DROP TABLE IF EXISTS step_edges;
CREATE TABLE step_edges (u UInt64, v UInt64, w Float64) ENGINE = Memory;
INSERT INTO step_edges VALUES (1, 2, 1), (1, 3, 1), (2, 4, 1), (3, 4, 5);

WITH RECURSIVE sp USING KEY (node) AS
(
    SELECT 1 :: UInt64 AS node, 0 :: Float64 AS cost
    UNION ALL
    SELECT e.v AS node, s.cost + e.w AS cost
    FROM sp s
    INNER JOIN step_edges e ON e.u = s.node
    LEFT JOIN sp_settled st ON st.node = e.v
    WHERE s.cost + e.w < if(st.node = 0, 1e308, st.cost)
    ORDER BY cost DESC
)
SELECT node, cost FROM sp ORDER BY node
SETTINGS max_threads = 1;

DROP TABLE step_edges;

-- A step that re-derives the same unchanged row many times retains its serialized key only once.
WITH RECURSIVE t USING KEY (k) AS
(
    SELECT repeat('x', 1024) AS k
    UNION ALL
    SELECT k FROM t CROSS JOIN numbers(200000) AS n
)
SELECT length(k) FROM t
SETTINGS max_threads = 1, max_block_size = 1024, max_joined_block_size_rows = 1024, max_memory_usage = 67108864;
